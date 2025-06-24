package mydist.process.abstraction;

import mydist.datastructures.distributed.DistributedAlg;
import mydist.datastructures.distributed.DistributedAlg.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

public class App implements AbstractionLayer{
    private final BlockingQueue<Message> messageQ;
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public App(BlockingQueue<Message> incomingMessages) {
        this.messageQ = incomingMessages;
    }

    @Override
    public void handleMessage(Message msg) {
        Message outgoingMessage = null;

        switch (msg.getType()) {
            case PL_DELIVER -> {
                PlDeliver plDeliver = msg.getPlDeliver();
                Message innerMsg = plDeliver.getMessage();
                switch (innerMsg.getType()) {
                    case APP_BROADCAST -> {
                        var value = innerMsg.getAppBroadcast().getValue();
                        logger.info("Received AppBroadcast({}), sending AppValue to all...", value.getV());

                        Message appValueMsg = Message.newBuilder()
                                .setType(Message.Type.APP_VALUE)
                                .setFromAbstractionId("app")
                                .setToAbstractionId("app")
                                .setAppValue(AppValue.newBuilder().setValue(value).build())
                                .build();
                        outgoingMessage = Message.newBuilder()
                                .setType(Message.Type.BEB_BROADCAST)
                                        .setFromAbstractionId("app")
                                        .setToAbstractionId("app.beb")
                                .setBebBroadcast(
                                        BebBroadcast.newBuilder()
                                                .setMessage(appValueMsg)
                                                .build()
                                ).build();
                    }
                    case APP_VALUE -> {
                        AppValue v = innerMsg.getAppValue();
                        logger.debug("Processing app value message for value: {}", v.getValue());
                        Message appValueMessage =
                                Message.newBuilder()
                                        .setType(Message.Type.APP_VALUE)
                                        .setAppValue(AppValue.newBuilder().setValue(v.getValue()).build())
                                        .build();
                        outgoingMessage = Message.newBuilder()
                                .setType(Message.Type.PL_SEND)
                                .setFromAbstractionId("app")
                                .setToAbstractionId("app.pl")
                                .setPlSend(
                                        PlSend.newBuilder()
                                                .setMessage(appValueMessage)
                                                .build()
                                ).build();
                    }
                    case APP_READ -> {
                        String register = innerMsg.getAppRead().getRegister();
                        logger.info("Read register '{}' request", register);
                        outgoingMessage = DistributedAlg.Message.newBuilder()
                                .setType(DistributedAlg.Message.Type.NNAR_READ)
                                .setFromAbstractionId("app")
                                .setToAbstractionId("app.nnar[" + register + "]")
                                .setNnarRead(DistributedAlg.NnarRead.newBuilder().build())
                                .build();

                    }
                    case APP_WRITE -> {
                        String register = innerMsg.getAppWrite().getRegister();
                        DistributedAlg.Value rawValue = innerMsg.getAppWrite().getValue();
                        logger.info("Write register '{}': {} request", register, rawValue.getV());

                        outgoingMessage = DistributedAlg.Message.newBuilder()
                                .setType(DistributedAlg.Message.Type.NNAR_WRITE)
                                .setSystemId(innerMsg.getSystemId())
                                .setFromAbstractionId("app")
                                .setToAbstractionId("app.nnar[" + register + "]")
                                .setNnarWrite(DistributedAlg.NnarWrite.newBuilder()
                                        .setValue(rawValue)
                                        .build())
                                .build();
                    }
                    case APP_PROPOSE -> {
                        var topic = innerMsg.getAppPropose().getTopic();
                        var value = innerMsg.getAppPropose().getValue();
                        logger.info("Received AppPropose({}, {}), creating UC_PROPOSE.", topic, value.getV());

                        outgoingMessage = Message.newBuilder()
                                .setType(Message.Type.UC_PROPOSE)
                                .setSystemId(innerMsg.getSystemId())
                                .setFromAbstractionId("app")
                                .setToAbstractionId("app.uc[" + topic + "]")
                                .setUcPropose(UcPropose.newBuilder().setValue(value).build())
                                .build();
                    }
                }
            }
            case BEB_DELIVER -> {
                BebDeliver bebDeliver = msg.getBebDeliver();
                DistributedAlg.Message delivered = bebDeliver.getMessage();
                DistributedAlg.ProcessId sender = bebDeliver.getSender();

                logger.debug("BEB_DELIVER received from {}-{} for {}", sender.getOwner(), sender.getIndex(), delivered.getType());

                PlSend plSend = PlSend.newBuilder().setMessage(
                        Message.newBuilder()
                        .setType(Message.Type.APP_VALUE)
                                .setAppValue(bebDeliver.getMessage().getAppValue())
                                .build()
                ).build();

                outgoingMessage = Message.newBuilder()
                        .setType(Message.Type.PL_SEND)
                        .setFromAbstractionId("app")
                        .setToAbstractionId("app.pl")
                        .setPlSend(plSend)
                        .build();
            }
            case NNAR_WRITE_RETURN -> {
                String register = extractRegisterFromAbstractionId(msg.getFromAbstractionId());

                logger.info("Write completed for register '{}'", register);

                DistributedAlg.Message appWriteReturnMsg = DistributedAlg.Message.newBuilder()
                        .setType(Message.Type.APP_WRITE_RETURN)
                        .setAppWriteReturn(DistributedAlg.AppWriteReturn.newBuilder()
                                .setRegister(register)
                                .build())
                        .build();
                PlSend plSend = PlSend.newBuilder().setMessage(
                        appWriteReturnMsg
                ).build();
                outgoingMessage = Message.newBuilder()
                        .setType(Message.Type.PL_SEND)
                        .setFromAbstractionId("app")
                        .setToAbstractionId("app.pl")
                        .setPlSend(plSend)
                        .build();

            }
            case NNAR_READ_RETURN -> {
                String register = extractRegisterFromAbstractionId(msg.getFromAbstractionId());
                DistributedAlg.Value value = msg.getNnarReadReturn().getValue();

                logger.info("Read register '{}' value: {}", register, value.getV());

                DistributedAlg.Message appReadReturnMsg = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.APP_READ_RETURN)
                        .setAppReadReturn(DistributedAlg.AppReadReturn.newBuilder()
                                .setRegister(register)
                                .setValue(value)
                                .build())
                        .build();
                PlSend plSend = PlSend.newBuilder().setMessage(
                        appReadReturnMsg
                ).build();
                outgoingMessage = Message.newBuilder()
                        .setType(Message.Type.PL_SEND)
                        .setFromAbstractionId("app")
                        .setToAbstractionId("app.pl")
                        .setPlSend(plSend)
                        .build();
            }
            case UC_DECIDE -> outgoingMessage = Message.newBuilder()
                    .setType(Message.Type.PL_SEND)
                    .setFromAbstractionId("app")
                    .setToAbstractionId("app.pl")
                    .setPlSend(PlSend.newBuilder()
                            .setMessage(Message.newBuilder()
                                    .setType(Message.Type.APP_DECIDE)
                                    .setToAbstractionId("app")
                                    .setAppDecide(AppDecide.newBuilder()
                                            .setValue(msg.getUcDecide().getValue())
                                            .build())
                                    .build())
                            .build())
                    .build();
        }
        if (outgoingMessage != null)
            messageQ.offer(outgoingMessage);
        else
            logger.warn("Outgoing Message is null, received message: {}", msg.getType());
    }

    @Override
    public void cleanup(){}

    private String extractRegisterFromAbstractionId(String abstractionId) {
        if (abstractionId.contains("app.nnar[") && abstractionId.contains("]")) {
            return abstractionId.substring(abstractionId.indexOf("[") + 1, abstractionId.indexOf("]"));
        }
        return "";
    }
}
