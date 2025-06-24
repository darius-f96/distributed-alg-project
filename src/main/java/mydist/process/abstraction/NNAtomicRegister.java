package mydist.process.abstraction;

import mydist.datastructures.distributed.DistributedAlg;
import mydist.datastructures.distributed.DistributedAlg.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class NNAtomicRegister implements AbstractionLayer{
    private static final Logger logger = LoggerFactory.getLogger(NNAtomicRegister.class);
    private final BlockingQueue<Message> messageQ;
    private final String registerKey;
    private final int totalProcesses;
    private final String owner;
    private int registerValue = -1;
    private final AtomicInteger readIdGen = new AtomicInteger(0);
    private int timestamp = 0;
    private final int writerRank;
    private final Map<Integer, OperationContext> activeOps = new ConcurrentHashMap<>();
    private record OperationContext(
            boolean writing,
            Value writeValue,
            Map<Integer, Value> readValues,
            Map<Integer, Integer> timestamps,
            Map<Integer, Integer> ranks,
            AtomicInteger readAcks,
            AtomicInteger writeAcks
    ) {}

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
                                        .setReadId(incomingReadId)
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
                int readId = readIdGen.incrementAndGet();
                DistributedAlg.Value value = DistributedAlg.Value.newBuilder()
                        .setDefined(true)
                        .setV(msg.getNnarWrite().getValue().getV())
                        .build();

                activeOps.put(readId, new OperationContext(
                        true,
                        value,
                        new ConcurrentHashMap<>(),
                        new ConcurrentHashMap<>(),
                        new ConcurrentHashMap<>(),
                        new AtomicInteger(0),
                        new AtomicInteger(0)
                ));

                logger.info("{}-{}: Starting NNAR_WRITE for register '{}', value: {}", owner, writerRank, registerKey, value.getV());

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

                int readId = readIdGen.incrementAndGet();

                activeOps.put(readId, new OperationContext(
                        false,
                        null,
                        new ConcurrentHashMap<>(),
                        new ConcurrentHashMap<>(),
                        new ConcurrentHashMap<>(),
                        new AtomicInteger(0),
                        new AtomicInteger(0)
                ));

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
                        OperationContext ctx = activeOps.get(readId);
                        if (ctx == null) {
                            int currentMaxReadId = readIdGen.get();

                            if (readId > currentMaxReadId) {
                                logger.debug("{}-{}: Early message for future readId {} > {}, requeuing.", owner, writerRank, readId, currentMaxReadId);
                                messageQ.offer(msg);
                            } else if (!activeOps.containsKey(readId)) {
                                logger.info("{}-{}: Ignoring unexpected message with readId {} (no matching operation)", owner, writerRank, readId);
                            }
                            return;
                        }

                        logger.debug("{}-{}: Received NNAR_INTERNAL_VALUE for register '{}', readId: {}, timestamp: {}, writerRank: {}, value: {}",
                                owner, writerRank, registerKey, readId, incomingTs, incomingWriterRank, value.getV());

                        ctx.readValues.put(incomingWriterRank, value);
                        ctx.timestamps.put(incomingWriterRank, incomingTs);
                        ctx.ranks.put(incomingWriterRank, incomingWriterRank);

                        if (ctx.readAcks.incrementAndGet() > totalProcesses / 2) {
                            logger.debug("{}-{}: Majority of processes responded for register '{}', readId: {}, ackCount: {}",
                                    owner, writerRank, registerKey, readId, ctx.readAcks.get());

                            int highestTs = 0;
                            int highestRank = 0;
                            DistributedAlg.Value highestValue = DistributedAlg.Value.newBuilder().setDefined(false).build();

                            for (int ts : ctx.timestamps.values()) {
                                if (ts > highestTs) {
                                    highestTs = ts;
                                }
                            }

                            logger.debug("{}-{}: Highest timestamp for register '{}': {}", owner, writerRank, registerKey, highestTs);

                            for (Map.Entry<Integer, Integer> entry : ctx.timestamps.entrySet()) {
                                if (entry.getValue() == highestTs) {
                                    int rank = ctx.ranks.get(entry.getKey());
                                    if (rank > highestRank) {
                                        highestRank = rank;
                                        highestValue = ctx.readValues.get(entry.getKey());
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
                            if (ctx.writing) {
                                int newTimestamp = highestTs + 1;

                                logger.info("{}-{}: This is a write operation for register '{}', readId: {}, valueToWrite: {}, newTimestamp: {}",
                                        owner, writerRank, registerKey, readId, ctx.writeValue.getV(), newTimestamp);

                                internalWriteMsg = DistributedAlg.Message.newBuilder()
                                        .setType(DistributedAlg.Message.Type.NNAR_INTERNAL_WRITE)
                                        .setSystemId(msg.getSystemId())
                                        .setFromAbstractionId(abstractionId)
                                        .setToAbstractionId(abstractionId)
                                        .setNnarInternalWrite(DistributedAlg.NnarInternalWrite.newBuilder()
                                                .setReadId(readId)
                                                .setTimestamp(newTimestamp)
                                                .setWriterRank(writerRank)
                                                .setValue(ctx.writeValue)
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
                        }
                    }
                    case NNAR_INTERNAL_ACK -> {
                        int incomingReadId = innerMsg.getNnarInternalAck().getReadId();
                        OperationContext ctx = activeOps.get(incomingReadId);
                        if (ctx == null) {
                            int currentMaxReadId = readIdGen.get();

                            if (incomingReadId > currentMaxReadId) {
                                logger.debug("{}-{}: Early message for future readId {} > {}, requeuing.", owner, writerRank, incomingReadId, currentMaxReadId);
                                messageQ.offer(msg);
                            } else if (!activeOps.containsKey(incomingReadId)) {
                                    logger.info("{}-{}: Ignoring unexpected message with readId {} (no matching operation)", owner, writerRank, incomingReadId);
                                }
                            return;
                        }
                        logger.debug("{}-{}: Handling ACK for read id '{}'", owner, writerRank, incomingReadId);
                            if (ctx.writeAcks.incrementAndGet() > totalProcesses / 2) {
                                if (ctx.writing){
                                    logger.info("{}-{}: WRITE quorum reached for readId {}, sending NNAR_WRITE_RETURN", owner, writerRank, incomingReadId);
                                    outgoingMessage = Message.newBuilder()
                                            .setType(Message.Type.NNAR_WRITE_RETURN)
                                            .setSystemId(msg.getSystemId())
                                            .setFromAbstractionId(abstractionId)
                                            .setToAbstractionId("app")
                                            .setNnarWriteReturn(NnarWriteReturn.getDefaultInstance())
                                            .build();
                                }else {
                                    logger.info("{}-{}: Returning NNAR_READ_RETURN with value={}, timestamp={}",
                                            owner, writerRank, registerValue, timestamp);
                                    outgoingMessage = Message.newBuilder()
                                            .setType(Message.Type.NNAR_READ_RETURN)
                                            .setFromAbstractionId(abstractionId)
                                            .setToAbstractionId("app")
                                            .setSystemId(msg.getSystemId())
                                            .setNnarReadReturn(
                                                    NnarReadReturn.newBuilder()
                                                                    .setValue(Value.newBuilder()
                                                                            .setV(registerValue)
                                                                            .setDefined(registerValue != -1)
                                                                            .build()
                                                                    ).build()
                                            )
                                            .build();


                                }
                                activeOps.remove(incomingReadId);
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
