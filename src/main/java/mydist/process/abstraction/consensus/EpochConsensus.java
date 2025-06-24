package mydist.process.abstraction.consensus;

import mydist.datastructures.distributed.DistributedAlg.*;
import mydist.process.abstraction.AbstractionLayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class EpochConsensus implements AbstractionLayer {
    private static final Logger logger = LoggerFactory.getLogger(EpochConsensus.class);
    private final String id;
    private final String parentId;
    private final BlockingQueue<Message> messageQ;
    private final List<ProcessId> processes;
    private boolean aborted = false;
    private final int ets;
    private EpState state;
    private Value tmpVal;
    private final Map<String, EpState> states = new ConcurrentHashMap<>();
    private int accepted = 0;

    public EpochConsensus(String parentId, String abstractionId, BlockingQueue<Message> messageQ,
                          List<ProcessId> processes, int ets, EpState initialState) {
        this.id = abstractionId;
        this.parentId = parentId;
        this.messageQ = messageQ;
        this.processes = processes;
        this.ets = ets;
        this.state = initialState;
        this.tmpVal = Value.getDefaultInstance();
    }

    @Override
    public void handleMessage(Message msg) {
        if (aborted) return;
        String systemId = msg.getSystemId();

        switch (msg.getType()) {
            case EP_ABORT -> {
                EpAborted abortedMsg = EpAborted.newBuilder()
                        .setEts(ets)
                        .setValueTimestamp(state.ets())
                        .setValue(state.value())
                        .build();

                messageQ.offer(Message.newBuilder()
                        .setType(Message.Type.EP_ABORTED)
                        .setSystemId(systemId)
                        .setFromAbstractionId(id)
                        .setToAbstractionId(parentId)
                        .setEpAborted(abortedMsg)
                        .build());

                aborted = true;
            }

            case EP_PROPOSE -> {
                tmpVal = msg.getEpPropose().getValue();
                Message internalReadMsg = Message.newBuilder()
                        .setType(Message.Type.EP_INTERNAL_READ)
                        .setSystemId(systemId)
                        .setFromAbstractionId(id)
                        .setToAbstractionId(id)
                        .setEpInternalRead(EpInternalRead.newBuilder().build())
                        .build();

                messageQ.offer(Message.newBuilder()
                        .setType(Message.Type.BEB_BROADCAST)
                        .setSystemId(systemId)
                        .setFromAbstractionId(id)
                        .setToAbstractionId(id + ".beb")
                        .setBebBroadcast(BebBroadcast.newBuilder().setMessage(internalReadMsg).build())
                        .build());
            }

            case BEB_DELIVER -> handleBebDeliver(systemId, msg);
            case PL_DELIVER -> handlePlDeliver(systemId, msg);
            default -> logger.warn("Unsupported EP message type: {}", msg.getType());
        }
    }

    private void handleBebDeliver(String systemId, Message msg) {
        Message inner = msg.getBebDeliver().getMessage();
        ProcessId sender = msg.getBebDeliver().getSender();

        switch (inner.getType()) {
            case EP_INTERNAL_READ -> {
                EpInternalState stateMsg = EpInternalState.newBuilder()
                        .setValueTimestamp(state.ets())
                        .setValue(state.value())
                        .build();

                messageQ.offer(Message.newBuilder()
                        .setType(Message.Type.PL_SEND)
                        .setSystemId(systemId)
                        .setFromAbstractionId(id)
                        .setToAbstractionId(id + ".pl")
                        .setPlSend(PlSend.newBuilder()
                                .setDestination(sender)
                                .setMessage(Message.newBuilder()
                                        .setType(Message.Type.EP_INTERNAL_STATE)
                                        .setSystemId(systemId)
                                        .setFromAbstractionId(id)
                                        .setToAbstractionId(id)
                                        .setEpInternalState(stateMsg)
                                        .build())
                                .build())
                        .build());
            }

            case EP_INTERNAL_WRITE -> {
                state = new EpState(ets, inner.getEpInternalWrite().getValue());

                messageQ.offer(Message.newBuilder()
                        .setType(Message.Type.PL_SEND)
                        .setSystemId(systemId)
                        .setFromAbstractionId(id)
                        .setToAbstractionId(id + ".pl")
                        .setPlSend(PlSend.newBuilder()
                                .setDestination(sender)
                                .setMessage(Message.newBuilder()
                                        .setType(Message.Type.EP_INTERNAL_ACCEPT)
                                        .setSystemId(systemId)
                                        .setFromAbstractionId(id)
                                        .setToAbstractionId(id)
                                        .build())
                                .build())
                        .build());
            }

            case EP_INTERNAL_DECIDED -> {
                messageQ.offer(Message.newBuilder()
                        .setType(Message.Type.EP_DECIDE)
                        .setSystemId(systemId)
                        .setFromAbstractionId(id)
                        .setToAbstractionId(parentId)
                        .setEpDecide(EpDecide.newBuilder()
                                .setEts(ets)
                                .setValue(state.value())
                                .build())
                        .build());
            }
            default -> logger.warn("Unsupported BEB inner message: {}", inner.getType());
        }
    }

    private void handlePlDeliver(String systemId, Message msg) {
        Message inner = msg.getPlDeliver().getMessage();
        ProcessId sender = msg.getPlDeliver().getSender();
        String key = sender.getOwner() + ":" + sender.getIndex();

        switch (inner.getType()) {
            case EP_INTERNAL_STATE -> {
                states.put(key, new EpState(inner.getEpInternalState().getValueTimestamp(), inner.getEpInternalState().getValue()));

                if (states.size() > processes.size() / 2) {
                    EpState highest = states.values().stream()
                            .max(Comparator.comparingInt(EpState::ets))
                            .orElse(state);

                    if (highest.value().getDefined()) {
                        tmpVal = highest.value();
                    }
                    states.clear();

                    messageQ.offer(Message.newBuilder()
                            .setType(Message.Type.BEB_BROADCAST)
                            .setSystemId(systemId)
                            .setFromAbstractionId(id)
                            .setToAbstractionId(id + ".beb")
                            .setBebBroadcast(BebBroadcast.newBuilder()
                                    .setMessage(Message.newBuilder()
                                            .setType(Message.Type.EP_INTERNAL_WRITE)
                                            .setSystemId(systemId)
                                            .setFromAbstractionId(id)
                                            .setToAbstractionId(id)
                                            .setEpInternalWrite(EpInternalWrite.newBuilder()
                                                    .setValue(tmpVal)
                                                    .build())
                                            .build())
                                    .build())
                            .build());
                }
            }

            case EP_INTERNAL_ACCEPT -> {
                accepted++;
                if (accepted > processes.size() / 2) {
                    accepted = 0;
                    messageQ.offer(Message.newBuilder()
                            .setType(Message.Type.BEB_BROADCAST)
                            .setSystemId(systemId)
                            .setFromAbstractionId(id)
                            .setToAbstractionId(id + ".beb")
                            .setBebBroadcast(BebBroadcast.newBuilder()
                                    .setMessage(Message.newBuilder()
                                            .setType(Message.Type.EP_INTERNAL_DECIDED)
                                            .setSystemId(systemId)
                                            .setFromAbstractionId(id)
                                            .setToAbstractionId(id)
                                            .setEpInternalDecided(EpInternalDecided.newBuilder()
                                                    .setValue(tmpVal)
                                                    .build())
                                            .build())
                                    .build())
                            .build());
                }
            }
            default -> logger.warn("Unsupported PL inner message: {}", inner.getType());
        }
    }

    @Override
    public void cleanup() {
    }
}
