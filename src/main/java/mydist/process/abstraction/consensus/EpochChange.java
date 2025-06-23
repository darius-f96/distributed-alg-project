package mydist.process.abstraction.consensus;

import mydist.datastructures.distributed.DistributedAlg.*;
import mydist.process.abstraction.AbstractionLayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class EpochChange implements AbstractionLayer {
    private static final Logger logger = LoggerFactory.getLogger(EpochChange.class);

    private final String id;
    private final String parentId;
    private final ProcessId self;
    private final BlockingQueue<Message> messageQ;
    private final List<ProcessId> processes;

    private ProcessId trusted;
    private int lastTs;
    private int ts;

    public EpochChange(String parentId, String id, ProcessId self,
                       BlockingQueue<Message> messageQ, List<ProcessId> processes) {
        this.id = id;
        this.parentId = parentId;
        this.self = self;
        this.messageQ = messageQ;
        this.processes = processes;

        this.trusted = processes.stream()
                .max(Comparator.comparingInt(ProcessId::getRank))
                .orElseThrow(() -> new IllegalArgumentException("Empty process list"));

        this.lastTs = 0;
        this.ts = self.getRank();
    }

    @Override
    public void handleMessage(Message msg) {
        switch (msg.getType()) {
            case ELD_TRUST -> {
                this.trusted = msg.getEldTrust().getProcess();
                handleSelfTrust(msg.getSystemId());
            }
            case PL_DELIVER -> {
                if (msg.getPlDeliver().getMessage().getType() == Message.Type.EC_INTERNAL_NACK) {
                    handleSelfTrust(msg.getSystemId());
                }
            }
            case BEB_DELIVER -> {
                Message inner = msg.getBebDeliver().getMessage();
                if (inner.getType() == Message.Type.EC_INTERNAL_NEW_EPOCH) {
                    int newTs = inner.getEcInternalNewEpoch().getTimestamp();
                    String senderKey = key(msg.getBebDeliver().getSender());
                    String trustedKey = key(this.trusted);

                    if (senderKey.equals(trustedKey) && newTs > this.lastTs) {
                        this.lastTs = newTs;

                        messageQ.offer(Message.newBuilder()
                                .setType(Message.Type.EC_START_EPOCH)
                                .setSystemId(msg.getSystemId())
                                .setFromAbstractionId(id)
                                .setToAbstractionId(parentId)
                                .setEcStartEpoch(EcStartEpoch.newBuilder()
                                        .setNewTimestamp(newTs)
                                        .setNewLeader(msg.getBebDeliver().getSender())
                                        .build())
                                .build());
                    } else {
                        Message nack = Message.newBuilder()
                                .setType(Message.Type.EC_INTERNAL_NACK)
                                .setSystemId(msg.getSystemId())
                                .setFromAbstractionId(id)
                                .setToAbstractionId(id)
                                .setEcInternalNack(EcInternalNack.getDefaultInstance())
                                .build();

                        messageQ.offer(Message.newBuilder()
                                .setType(Message.Type.PL_SEND)
                                .setSystemId(msg.getSystemId())
                                .setFromAbstractionId(id)
                                .setToAbstractionId(id + ".pl")
                                .setPlSend(PlSend.newBuilder()
                                        .setMessage(nack)
                                        .build())
                                .build());
                    }
                } else {
                    logger.warn("[EC] Unknown BEB_DELIVER message type: {}", inner.getType());
                }
            }
            default -> logger.warn("[EC] Unknown message type: {}", msg.getType());
        }
    }

    private void handleSelfTrust(String systemId) {
        if (key(self).equals(key(trusted))) {
            this.ts = lastTs + processes.size();

            Message inner = Message.newBuilder()
                    .setType(Message.Type.EC_INTERNAL_NEW_EPOCH)
                    .setSystemId(systemId)
                    .setFromAbstractionId(id)
                    .setToAbstractionId(id)
                    .setEcInternalNewEpoch(EcInternalNewEpoch.newBuilder()
                            .setTimestamp(ts)
                            .build())
                    .build();

            messageQ.offer(Message.newBuilder()
                    .setType(Message.Type.BEB_BROADCAST)
                    .setSystemId(systemId)
                    .setFromAbstractionId(id)
                    .setToAbstractionId(id + ".beb")
                    .setBebBroadcast(BebBroadcast.newBuilder()
                            .setMessage(inner)
                            .build())
                    .build());
        }
    }

    private String key(ProcessId p) {
        return p.getOwner() + "-" + p.getIndex();
    }

    @Override
    public void cleanup() {
    }
}
