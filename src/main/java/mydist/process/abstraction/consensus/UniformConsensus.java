package mydist.process.abstraction.consensus;

import mydist.datastructures.distributed.DistributedAlg.*;
import mydist.process.abstraction.AbstractionLayer;
import mydist.process.abstraction.BestEffortBroadcast;
import mydist.process.abstraction.PerfectLink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;

public class UniformConsensus implements AbstractionLayer {
    private static final Logger logger = LoggerFactory.getLogger(UniformConsensus.class);

    private final String abstractionId;
    private final BlockingQueue<Message> messageQ;
    private final Map<String, AbstractionLayer> abstractions;
    private final List<ProcessId> processes;
    private final int index;
    private final String owner;
    private final PerfectLink pl;

    private Value val = Value.getDefaultInstance();
    private boolean proposed = false;
    private boolean decided = false;
    private int ets = 0;
    private ProcessId l;
    private int newTs;
    private ProcessId newL;

    public UniformConsensus(String abstractionId,
                            BlockingQueue<Message> messageQ,
                            Map<String, AbstractionLayer> abstractions,
                            List<ProcessId> processes,
                            String owner,
                            int index,
                            PerfectLink pl) {
        this.abstractionId = abstractionId;
        this.messageQ = messageQ;
        this.abstractions = abstractions;
        this.processes = processes;
        this.index = index;
        this.owner = owner;
        this.pl = pl;
        this.l = processes.stream().max(Comparator.comparingInt(ProcessId::getRank)).orElseThrow();

        addEpAbstractions(new EpState(0, val));
    }

    @Override
    public void handleMessage(Message m) {
        String systemId = m.getSystemId();
        logger.info("[UC] Handling message {} at ets={}, decided={}", m.getType(), ets, decided);

        switch (m.getType()) {
            case UC_PROPOSE -> {
                val = m.getUcPropose().getValue();
                logger.info("[UC] Proposing value {}", val.getV());
            }
            case EC_START_EPOCH -> {
                if (m.getEcStartEpoch().getNewTimestamp() > ets) {
                    logger.info("[UC] Received EC_START_EPOCH with ts={}, current ets={}", m.getEcStartEpoch().getNewTimestamp(), ets);
                    newTs = m.getEcStartEpoch().getNewTimestamp();
                    newL = m.getEcStartEpoch().getNewLeader();

                    messageQ.offer(Message.newBuilder()
                            .setType(Message.Type.EP_ABORT)
                            .setSystemId(systemId)
                            .setFromAbstractionId(abstractionId)
                            .setToAbstractionId(abstractionId + getEpId())
                            .setEpAbort(EpAbort.getDefaultInstance())
                            .build());
                }
            }
            case EP_ABORTED -> {
                if (ets == m.getEpAborted().getEts()) {
                    logger.info("[UC] EpAborted: current ets={}, message ets={}", ets, m.getEpAborted().getEts());
                    if (newTs > 0 && newL != null) {
                        ets = newTs;
                        l = newL;
                        proposed = false;

                        addEpAbstractions(new EpState(ets, m.getEpAborted().getValue()));
                    } else {
                        logger.warn("[UC] Received EP_ABORTED but newTs or newL not initialized");
                    }
                }
            }
            case EP_DECIDE -> {
                if (ets == m.getEpDecide().getEts() && !decided) {
                    decided = true;
                    logger.info("[UC] DECIDE value: {}", m.getEpDecide().getValue().getV());

                    messageQ.offer(Message.newBuilder()
                            .setType(Message.Type.UC_DECIDE)
                            .setSystemId(systemId)
                            .setFromAbstractionId(abstractionId)
                            .setToAbstractionId("app")
                            .setUcDecide(UcDecide.newBuilder()
                                    .setValue(m.getEpDecide().getValue())
                                    .build())
                            .build());
                }
            }
            default -> throw new UnsupportedOperationException("Unsupported UC message type: " + m.getType());
        }

        updateLeader(systemId);
    }

    private void updateLeader(String systemId) {
        if (Objects.equals(l.getOwner(), owner)
                && l.getIndex() == index
                && val.getDefined()
                && !proposed
                && !decided) {
            logger.info("[UC] Proposing as leader {}-{} with value {}", owner, index, val.getV());
            proposed = true;

            messageQ.offer(Message.newBuilder()
                    .setType(Message.Type.EP_PROPOSE)
                    .setSystemId(systemId)
                    .setFromAbstractionId(abstractionId)
                    .setToAbstractionId(abstractionId + getEpId())
                    .setEpPropose(EpPropose.newBuilder()
                            .setValue(val)
                            .build())
                    .build());
        }
    }

    private void addEpAbstractions(EpState initialState) {
        String epId = getEpId();
        String epRoot = abstractionId + epId;
        logger.info("[UC] Creating new epoch abstractions for epoch {}", ets);

        abstractions.put(epRoot, new EpochConsensus(abstractionId, epRoot, messageQ, processes, ets, initialState));
        abstractions.put(epRoot + ".beb", new BestEffortBroadcast(messageQ, processes, epRoot + ".beb"));
        abstractions.put(epRoot + ".pl", pl.createCopyWithParentAbstractionId(epRoot));
        abstractions.put(epRoot + ".beb.pl", pl.createCopyWithParentAbstractionId(epRoot + ".beb"));
    }

    private String getEpId() {
        return ".ep[" + ets + "]";
    }

    @Override
    public void cleanup() {
    }
}
