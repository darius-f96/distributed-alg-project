package mydist.process.abstraction;

import mydist.datastructures.distributed.DistributedAlg;
import mydist.datastructures.distributed.DistributedAlg.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class NNAtomicRegister implements AbstractionLayer{
    private static final Logger logger = LoggerFactory.getLogger(NNAtomicRegister.class);
    private final BlockingQueue<Message> messageQ;
    private final String registerKey;
    private final int totalProcesses;
    private final String owner;
    private int registerValue = -1;
    private int readId = 0;
    private Value writeValue;
    private Map<Integer, Value> readValuesMap;
    private Map<Integer, Integer> readTimestampsMap;
    private Map<Integer, Integer> readWriterRanksMap;
    private int ackCount;
    private int timestamp = 0;
    private final int writerRank;
    private boolean writing = false;

    public NNAtomicRegister(final BlockingQueue<Message> messageQ, String registerKey, int writerRank, int totalProcesses, String owner) {
        this.messageQ = messageQ;
        this.registerKey = registerKey;
        this.writerRank = writerRank;
        this.totalProcesses = totalProcesses;
        this.owner = owner;
    }

    @Override
    public void handleMessage(Message msg) {
        Message outgoingMessage = null;
        String abstractionId = getAbstractionId();
        switch(msg.getType()) {
            case BEB_DELIVER -> {
                BebDeliver bebDeliver = msg.getBebDeliver();
                Message innerMsg = bebDeliver.getMessage();
                switch (innerMsg.getType()) {
                    case NNAR_INTERNAL_READ -> {
                        int incomingReadId = innerMsg.getNnarInternalRead().getReadId();

                        boolean defined = registerValue != -1;
                        Value value = Value.newBuilder()
                                .setV(registerValue)
                                .setDefined(defined)
                                .build();

                        logger.debug("{}-{}: NNAR_INTERNAL_READ for register '{}', readId: {}, local value: {}, timestamp: {}, writerRank: {}",
                                owner, writerRank, registerKey, incomingReadId, value.getV(), timestamp, writerRank);

                        DistributedAlg.Message internalValueMsg = DistributedAlg.Message.newBuilder()
                                .setType(DistributedAlg.Message.Type.NNAR_INTERNAL_VALUE)
                                .setSystemId(innerMsg.getSystemId())
                                .setFromAbstractionId(abstractionId)
                                .setToAbstractionId(abstractionId)
                                .setNnarInternalValue(DistributedAlg.NnarInternalValue.newBuilder()
                                        .setReadId(incomingReadId)
                                        .setTimestamp(timestamp)
                                        .setWriterRank(writerRank)
                                        .setValue(value)
                                        .build())
                                .build();

                        PlSend plSend = PlSend.newBuilder()
                                .setDestination(bebDeliver.getSender())
                                .setMessage(internalValueMsg)
                                .build();
                        outgoingMessage = Message.newBuilder()
                                .setType(Message.Type.PL_SEND)
                                .setFromAbstractionId(abstractionId)
                                .setToAbstractionId(abstractionId + ".pl")
                                .setSystemId(msg.getSystemId())
                                .setPlSend(plSend)
                                .build();
                    }
                    case NNAR_INTERNAL_WRITE -> {
                        int incomingReadId = innerMsg.getNnarInternalWrite().getReadId();
                        int incomingTimestamp = innerMsg.getNnarInternalWrite().getTimestamp();
                        DistributedAlg.Value value = innerMsg.getNnarInternalWrite().getValue();

                        logger.info("{}-{}: Writing register '{}', readId: {}, timestamp: {}, value: {}",
                                owner, writerRank, registerKey, incomingReadId, timestamp, value.getV());

                        registerValue = value.getV();
                        timestamp = incomingTimestamp;

                        DistributedAlg.Message ackMsg = DistributedAlg.Message.newBuilder()
                                .setType(DistributedAlg.Message.Type.NNAR_INTERNAL_ACK)
                                .setSystemId(msg.getSystemId())
                                .setFromAbstractionId(abstractionId)
                                .setToAbstractionId(abstractionId)
                                .setNnarInternalAck(NnarInternalAck.newBuilder()
                                        .setReadId(readId)
                                        .build())
                                .build();
                        PlSend plSend = PlSend.newBuilder()
                                .setDestination(bebDeliver.getSender())
                                .setMessage(ackMsg)
                                .build();
                        outgoingMessage = Message.newBuilder()
                                .setType(Message.Type.PL_SEND)
                                .setFromAbstractionId(abstractionId)
                                .setToAbstractionId(abstractionId + ".pl")
                                .setSystemId(msg.getSystemId())
                                .setPlSend(plSend)
                                .build();
                    }
                }
            }
            case NNAR_WRITE -> {
                DistributedAlg.Value rawValue = msg.getNnarWrite().getValue();
                DistributedAlg.Value value = DistributedAlg.Value.newBuilder()
                        .setDefined(true)
                        .setV(rawValue.getV())
                        .build();

                logger.info("{}-{}: Starting NNAR_WRITE for register '{}', value: {}", owner, writerRank, registerKey, value.getV());

                readId++;

                readValuesMap = new ConcurrentHashMap<>();
                readTimestampsMap = new ConcurrentHashMap<>();
                readWriterRanksMap = new ConcurrentHashMap<>();
                ackCount = 0;

                readValuesMap.put(readId, value);
                writing = true;
                writeValue = value;

                DistributedAlg.Message internalReadMsg = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.NNAR_INTERNAL_READ)
                        .setSystemId(msg.getSystemId())
                        .setFromAbstractionId(abstractionId)
                        .setToAbstractionId(abstractionId)
                        .setNnarInternalRead(DistributedAlg.NnarInternalRead.newBuilder()
                                .setReadId(readId)
                                .build())
                        .build();

                outgoingMessage = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.BEB_BROADCAST)
                        .setSystemId(msg.getSystemId())
                        .setFromAbstractionId(abstractionId)
                        .setToAbstractionId(abstractionId+ ".beb")
                        .setBebBroadcast(DistributedAlg.BebBroadcast.newBuilder()
                                .setMessage(internalReadMsg)
                                .build())
                        .build();
            }
            case NNAR_READ -> {

                logger.info("{}-{}: Starting NNAR_READ for register '{}'", owner, writerRank, registerKey);

                readId++;

                readValuesMap = new ConcurrentHashMap<>();
                readTimestampsMap = new ConcurrentHashMap<>();
                readWriterRanksMap = new ConcurrentHashMap<>();
                ackCount = 0;

                DistributedAlg.Message internalReadMsg = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.NNAR_INTERNAL_READ)
                        .setSystemId(msg.getSystemId())
                        .setFromAbstractionId(abstractionId)
                        .setToAbstractionId(abstractionId)
                        .setNnarInternalRead(DistributedAlg.NnarInternalRead.newBuilder()
                                .setReadId(readId)
                                .build())
                        .build();

                outgoingMessage = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.BEB_BROADCAST)
                        .setSystemId(msg.getSystemId())
                        .setFromAbstractionId(abstractionId)
                        .setToAbstractionId(abstractionId + ".beb")
                        .setBebBroadcast(DistributedAlg.BebBroadcast.newBuilder()
                                .setMessage(internalReadMsg)
                                .build())
                        .build();
            }
            case PL_DELIVER -> {
                Message innerMsg = msg.getPlDeliver().getMessage();
                switch (innerMsg.getType()) {
                    case NNAR_INTERNAL_VALUE -> {
                        int readId = innerMsg.getNnarInternalValue().getReadId();
                        int incomingTs = innerMsg.getNnarInternalValue().getTimestamp();
                        int incomingWriterRank = innerMsg.getNnarInternalValue().getWriterRank();
                        DistributedAlg.Value value = innerMsg.getNnarInternalValue().getValue();

                        logger.debug("{}-{}: Received NNAR_INTERNAL_VALUE for register '{}', readId: {}, timestamp: {}, writerRank: {}, value: {}",
                                owner, writerRank, registerKey, readId, incomingTs, incomingWriterRank, value.getV());

                        readValuesMap.put(readId, value);
                        readTimestampsMap.put(readId, incomingTs);
                        readWriterRanksMap.put(readId, incomingWriterRank);

                        ackCount++;

                        if (ackCount > totalProcesses / 2) {
                            logger.debug("{}-{}: Majority of processes responded for register '{}', readId: {}, ackCount: {}",
                                    owner, writerRank, registerKey, readId, ackCount);

                            int highestTs = 0;
                            int highestRank = 0;
                            DistributedAlg.Value highestValue = DistributedAlg.Value.newBuilder().setDefined(false).build();

                            for (int ts : readTimestampsMap.values()) {
                                if (ts > highestTs) {
                                    highestTs = ts;
                                }
                            }

                            logger.debug("{}-{}: Highest timestamp for register '{}': {}", owner, writerRank, registerKey, highestTs);

                            for (Map.Entry<Integer, Integer> entry : readTimestampsMap.entrySet()) {
                                if (entry.getValue() == highestTs) {
                                    int rank = readWriterRanksMap.get(entry.getKey());
                                    if (rank > highestRank) {
                                        highestRank = rank;
                                        highestValue = readValuesMap.get(entry.getKey());
                                        logger.debug("{}-{}: New highest rank for register '{}': {}, value: {}",
                                                owner, writerRank, registerKey, highestRank, highestValue.getV());
                                    }
                                }
                            }

                            registerValue = highestValue.getV();
                            timestamp = highestTs;

                            logger.info("{}-{}: Updated local value for register '{}': {}, timestamp: {}",
                                    owner, writerRank, registerKey, highestValue.getV(), highestTs);
                            DistributedAlg.Message internalWriteMsg;
                            if (writing) {
                                int newTimestamp = highestTs + 1;

                                logger.info("{}-{}: This is a write operation for register '{}', readId: {}, valueToWrite: {}, newTimestamp: {}",
                                        owner, writerRank, registerKey, readId, writeValue.getV(), newTimestamp);

                                internalWriteMsg = DistributedAlg.Message.newBuilder()
                                        .setType(DistributedAlg.Message.Type.NNAR_INTERNAL_WRITE)
                                        .setSystemId(msg.getSystemId())
                                        .setFromAbstractionId(abstractionId)
                                        .setToAbstractionId(abstractionId)
                                        .setNnarInternalWrite(DistributedAlg.NnarInternalWrite.newBuilder()
                                                .setReadId(readId)
                                                .setTimestamp(newTimestamp)
                                                .setWriterRank(writerRank)
                                                .setValue(writeValue)
                                                .build())
                                        .build();
                            } else {
                                logger.info("{}-{}: This is a read operation for register '{}', readId: {}, returning value: {}",
                                        owner, writerRank, registerKey, readId, highestValue.getV());

                                 internalWriteMsg = DistributedAlg.Message.newBuilder()
                                        .setType(Message.Type.NNAR_INTERNAL_WRITE)
                                        .setSystemId(msg.getSystemId())
                                        .setFromAbstractionId(abstractionId)
                                        .setToAbstractionId(abstractionId)
                                        .setNnarInternalWrite(DistributedAlg.NnarInternalWrite.newBuilder()
                                                .setReadId(readId)
                                                .setTimestamp(highestTs)
                                                .setWriterRank(writerRank)
                                                .setValue(highestValue)
                                                .build())
                                        .build();

                            }
                            outgoingMessage = DistributedAlg.Message.newBuilder()
                                    .setType(DistributedAlg.Message.Type.BEB_BROADCAST)
                                    .setSystemId(msg.getSystemId())
                                    .setFromAbstractionId(abstractionId)
                                    .setToAbstractionId(abstractionId + ".beb")
                                    .setBebBroadcast(BebBroadcast.newBuilder()
                                            .setMessage(internalWriteMsg)
                                            .build())
                                    .build();
                            readValuesMap.remove(readId);
                            readTimestampsMap.remove(readId);
                            readWriterRanksMap.remove(readId);
                            ackCount = 0;
                        }
                    }
                    case NNAR_INTERNAL_ACK -> {
                        int incomingReadId = innerMsg.getNnarInternalAck().getReadId();
                        logger.debug("{}-{}: Handling ACK for read id '{}'", owner, writerRank, incomingReadId);
                        ackCount++;
                        if (incomingReadId == readId) {
                            if (ackCount > totalProcesses / 2) {
                                ackCount = 0;
                                if (writing){
                                    writing = false;
                                    writeValue = null;
                                    outgoingMessage = Message.newBuilder()
                                            .setType(Message.Type.NNAR_WRITE_RETURN)
                                            .setSystemId(msg.getSystemId())
                                            .setFromAbstractionId(abstractionId)
                                            .setToAbstractionId("app")
                                            .setNnarWriteReturn(NnarWriteReturn.getDefaultInstance())
                                            .build();
                                } else
                                    outgoingMessage = Message.newBuilder()
                                            .setType(Message.Type.NNAR_READ_RETURN)
                                            .setFromAbstractionId(abstractionId)
                                            .setToAbstractionId("app")
                                            .setSystemId(msg.getSystemId())
                                            .setNnarReadReturn(
                                                    NnarReadReturn.newBuilder()
                                                            .setValue(NnarInternalValue.newBuilder()
                                                                    .setReadId(incomingReadId)
                                                                    .setTimestamp(timestamp)
                                                                    .setWriterRank(writerRank)
                                                                    .setValue(Value.newBuilder()
                                                                            .setV(registerValue)
                                                                            .setDefined(registerValue != -1)
                                                                            .build()
                                                                    ).build()
                                                                    .getValue()
                                                            )
                                                            .build()
                                            )
                                            .build();
                            }
                        }
                    }
                }
            }
        }
        if (outgoingMessage != null) {
            messageQ.offer(outgoingMessage);
        }
    }

    @Override
    public void cleanup() {

    }

    private String getAbstractionId() {
        return "app.nnar[" + registerKey + "]";
    }
}
