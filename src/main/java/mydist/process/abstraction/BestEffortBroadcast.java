package mydist.process.abstraction;

import mydist.datastructures.distributed.DistributedAlg;
import mydist.datastructures.distributed.DistributedAlg.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class BestEffortBroadcast implements AbstractionLayer{
    private static final Logger logger = LoggerFactory.getLogger(BestEffortBroadcast.class);
    private final BlockingQueue<Message> messageQ;
    private final List<ProcessId> processes;
    private final String processId;

    public BestEffortBroadcast(BlockingQueue<Message> messageQ, List<ProcessId> processes, String processId) {
        this.messageQ = messageQ;
        this.processes = processes;
        this.processId = processId;
    }

    @Override
    public void handleMessage(Message msg) {
        Message outgoingMessage = null;

        switch (msg.getType()) {
            case BEB_BROADCAST -> {
                BebBroadcast bebBroadcast = msg.getBebBroadcast();
                for (ProcessId pid : processes) {
                   outgoingMessage = DistributedAlg.Message.newBuilder()
                            .setType(Message.Type.PL_SEND)

                            .setFromAbstractionId(processId)
                            .setToAbstractionId(processId + ".pl")
                           .setPlSend(
                                    PlSend.newBuilder()
                                            .setDestination(pid)
                                            .setMessage(bebBroadcast.getMessage())
                                    .build())
                           .build();
                    messageQ.offer(outgoingMessage);
                }
            }
            case PL_DELIVER -> {
                BebDeliver bebDeliver = BebDeliver.newBuilder()
                        .setSender(msg.getPlDeliver().getSender())
                        .setMessage(msg.getPlDeliver().getMessage())
                        .build();
                outgoingMessage = Message.newBuilder()
                        .setType(Message.Type.BEB_DELIVER)
                        .setFromAbstractionId(processId)
                        .setToAbstractionId(msg.getPlDeliver().getMessage().getToAbstractionId())
                        .setBebDeliver(bebDeliver)
                        .build();

                messageQ.offer(outgoingMessage);
            }
        }
    }

    @Override
    public void cleanup() {

    }
}
