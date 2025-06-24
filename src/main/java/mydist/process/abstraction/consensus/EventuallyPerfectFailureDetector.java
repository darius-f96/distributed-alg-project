package mydist.process.abstraction.consensus;

import mydist.datastructures.distributed.DistributedAlg.*;
import mydist.process.abstraction.AbstractionLayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class EventuallyPerfectFailureDetector implements AbstractionLayer {
    private static final Logger logger = LoggerFactory.getLogger(EventuallyPerfectFailureDetector.class);

    private final String id;
    private final String parentId;
    private final BlockingQueue<Message> messageQ;
    private final List<ProcessId> processes;

    private final Map<String, ProcessId> alive = new ConcurrentHashMap<>();
    private final Map<String, ProcessId> suspected = new ConcurrentHashMap<>();

    private int delay = DELTA;
    private static final int DELTA = 100;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public EventuallyPerfectFailureDetector(String parentId, String id, BlockingQueue<Message> messageQ, List<ProcessId> processes) {
        this.id = id;
        this.parentId = parentId;
        this.messageQ = messageQ;
        this.processes = processes;

        for (ProcessId p : processes) {
            alive.put(getProcessKey(p), p);
        }

        scheduleTimeout();
    }

    private void scheduleTimeout() {
        scheduler.schedule(() -> {
            messageQ.offer(Message.newBuilder()
                    .setType(Message.Type.EPFD_TIMEOUT)
                    .setFromAbstractionId(id)
                    .setToAbstractionId(id)
                    .setEpfdTimeout(EpfdTimeout.getDefaultInstance())
                    .build());
        }, delay, TimeUnit.MILLISECONDS);
    }

    private static String getProcessKey(ProcessId p) {
        return p.getOwner() + ":" + p.getIndex();
    }

    @Override
    public void handleMessage(Message msg) {
        switch (msg.getType()) {
            case EPFD_TIMEOUT -> handleTimeout(msg.getSystemId());

            case PL_DELIVER -> {
                Message inner = msg.getPlDeliver().getMessage();
                switch (inner.getType()) {
                    case EPFD_INTERNAL_HEARTBEAT_REQUEST -> sendHeartbeatReply(msg.getSystemId());

                    case EPFD_INTERNAL_HEARTBEAT_REPLY -> {
                        ProcessId sender = msg.getPlDeliver().getSender();
                        alive.put(getProcessKey(sender), sender);
                    }
                    default -> logger.warn("Unsupported PL_DELIVER message type: {}", inner.getType());
                }
            }
            default -> logger.warn("Unsupported EPFD message type: {}", msg.getType());
        }
    }

    private void sendHeartbeatReply(String systemId) {
        Message reply = Message.newBuilder()
                .setType(Message.Type.PL_SEND)
                .setSystemId(systemId)
                .setFromAbstractionId(id)
                .setToAbstractionId(id + ".pl")
                .setPlSend(PlSend.newBuilder()
                        .setMessage(Message.newBuilder()
                                .setType(Message.Type.EPFD_INTERNAL_HEARTBEAT_REPLY)
                                .setSystemId(systemId)
                                .setFromAbstractionId(id)
                                .setToAbstractionId(id)
                                .setEpfdInternalHeartbeatReply(EpfdInternalHeartbeatReply.getDefaultInstance())
                                .build())
                        .build())
                .build();

        messageQ.offer(reply);
    }

    private void handleTimeout(String systemId) {
        boolean delayIncreased = false;

        for (ProcessId p : processes) {
            String key = getProcessKey(p);
            boolean isAlive = alive.containsKey(key);
            boolean isSuspected = suspected.containsKey(key);

            if (!isAlive && !isSuspected) {
                suspected.put(key, p);
                messageQ.offer(Message.newBuilder()
                        .setType(Message.Type.EPFD_SUSPECT)
                        .setSystemId(systemId)
                        .setFromAbstractionId(id)
                        .setToAbstractionId(parentId)
                        .setEpfdSuspect(EpfdSuspect.newBuilder().setProcess(p).build())
                        .build());
            } else if (isAlive && isSuspected) {
                suspected.remove(key);
                messageQ.offer(Message.newBuilder()
                        .setType(Message.Type.EPFD_RESTORE)
                        .setSystemId(systemId)
                        .setFromAbstractionId(id)
                        .setToAbstractionId(parentId)
                        .setEpfdRestore(EpfdRestore.newBuilder().setProcess(p).build())
                        .build());
            }

            messageQ.offer(Message.newBuilder()
                    .setType(Message.Type.PL_SEND)
                    .setFromAbstractionId(id)
                    .setToAbstractionId(id + ".pl")
                    .setPlSend(PlSend.newBuilder()
                            .setDestination(p)
                            .setMessage(
                                Message.newBuilder()
                                .setType(Message.Type.EPFD_INTERNAL_HEARTBEAT_REQUEST)
                                .setSystemId(systemId)
                                .setFromAbstractionId(id)
                                .setToAbstractionId(id)
                                .setEpfdInternalHeartbeatRequest(EpfdInternalHeartbeatRequest.getDefaultInstance())
                                .build())
                            .build())
                    .build());

            if (isAlive && isSuspected) delayIncreased = true;
        }

        if (delayIncreased) delay += DELTA;

        alive.clear();
        scheduleTimeout();
    }

    @Override
    public void cleanup() {
        scheduler.shutdownNow();
    }
}
