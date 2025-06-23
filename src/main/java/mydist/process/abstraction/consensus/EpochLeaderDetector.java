package mydist.process.abstraction.consensus;

import mydist.datastructures.distributed.DistributedAlg;
import mydist.datastructures.distributed.DistributedAlg.Message;
import mydist.datastructures.distributed.DistributedAlg.EldTrust;
import mydist.datastructures.distributed.DistributedAlg.EpfdSuspect;
import mydist.datastructures.distributed.DistributedAlg.EpfdRestore;
import mydist.datastructures.distributed.DistributedAlg.ProcessId;
import mydist.process.abstraction.AbstractionLayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;

public class EpochLeaderDetector implements AbstractionLayer {
    private static final Logger logger = LoggerFactory.getLogger(EpochLeaderDetector.class);
    private final String abstractionId;
    private final String parentAbstractionId;
    private final BlockingQueue<Message> messageQueue;
    private final List<ProcessId> processes;
    private final Map<String, ProcessId> processMap = new HashMap<>();
    private final Set<String> suspected = new HashSet<>();
    private ProcessId currentLeader = null;

    public EpochLeaderDetector(
                               BlockingQueue<Message> messageQueue,
                               String parentAbstractionId,
                               String abstractionId,
                               List<ProcessId> processes) {
        this.abstractionId = abstractionId;
        this.parentAbstractionId = parentAbstractionId;
        this.messageQueue = messageQueue;
        this.processes = new ArrayList<>(processes);
        for (ProcessId p : processes) {
            processMap.put(p.getOwner() + ":" + p.getIndex(), p);
        }
        electNewLeader();
    }

    @Override
    public void handleMessage(Message msg) {
        switch (msg.getType()) {
            case EPFD_SUSPECT -> handleSuspect(msg.getEpfdSuspect());
            case EPFD_RESTORE -> handleRestore(msg.getEpfdRestore());
            default -> logger.warn("Unhandled message type in ELD: {}", msg.getType());
        }
    }

    private void handleSuspect(EpfdSuspect suspect) {
        String pidKey = key(suspect.getProcess());
        if (suspected.add(pidKey)) {
            logger.info("[ELD] Suspecting {}", pidKey);
            electNewLeader();
        }
    }

    private void handleRestore(EpfdRestore restore) {
        String pidKey = key(restore.getProcess());
        if (suspected.remove(pidKey)) {
            logger.info("[ELD] Restoring {}", pidKey);
            electNewLeader();
        }
    }

    private void electNewLeader() {
        ProcessId newLeader = processes.stream()
                .filter(p -> !suspected.contains(key(p)))
                .max(Comparator.comparingInt(ProcessId::getRank))
                .orElse(null);

        if (newLeader != null && (currentLeader == null || !key(currentLeader).equals(key(newLeader)))) {
            currentLeader = newLeader;
            logger.info("[ELD] New leader elected: {}:{}", newLeader.getOwner(), newLeader.getIndex());

            Message trustMsg = Message.newBuilder()
                    .setType(Message.Type.ELD_TRUST)
                    .setFromAbstractionId(abstractionId)
                    .setToAbstractionId("ec")
                    .setSystemId(parentAbstractionId.split("\\.")[0])
                    .setEldTrust(EldTrust.newBuilder()
                            .setProcess(currentLeader)
                            .build())
                    .build();
            messageQueue.offer(trustMsg);
        }
    }

    private String key(ProcessId pid) {
        return pid.getOwner() + ":" + pid.getIndex();
    }

    @Override
    public void cleanup() {
    }
}
