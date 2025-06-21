package mydist.process;

import mydist.datastructures.distributed.DistributedAlg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class DistributedProcess {
    private static final Logger logger = LoggerFactory.getLogger(DistributedProcess.class);

    private final String owner;
    private final int index;
    private final String host;
    private final int port;
    private final String hubHost;
    private final int hubPort;
    private String systemId = "";
    private List<DistributedAlg.ProcessId> processes;
    private Map<String, DistributedAlg.Value> registerMap;
    private Map<String, Integer> readIdMap;
    private Map<String, Map<Integer, DistributedAlg.Value>> readValuesMap;
    private Map<String, Map<Integer, Integer>> readTimestampsMap;
    private Map<String, Map<Integer, Integer>> readWriterRanksMap;
    private Map<String, Map<Integer, Integer>> ackCountMap;
    private Map<String, Integer> timestampMap;
    private Map<String, Set<Integer>> writeReadIdsMap;
    private Map<String, Map<Integer, DistributedAlg.Value>> writeValueMap;

    public DistributedProcess(String owner, int index, String host, int port, String hubHost, int hubPort) {
        this.owner = owner;
        this.index = index;
        this.host = host;
        this.port = port;
        this.hubHost = hubHost;
        this.hubPort = hubPort;
        this.registerMap = new ConcurrentHashMap<>();
        this.readIdMap = new ConcurrentHashMap<>();
        this.readValuesMap = new ConcurrentHashMap<>();
        this.readTimestampsMap = new ConcurrentHashMap<>();
        this.readWriterRanksMap = new ConcurrentHashMap<>();
        this.ackCountMap = new ConcurrentHashMap<>();
        this.timestampMap = new ConcurrentHashMap<>();
    }

    public Runnable start() throws IOException {
        registerToHub();
        startTcpListener();
        return null;
    }

    private void registerToHub() throws IOException {
        DistributedAlg.Message outer = DistributedAlg.Message.newBuilder()
                .setType(DistributedAlg.Message.Type.NETWORK_MESSAGE)
                .setToAbstractionId("app")
                .setSystemId("")
                .setNetworkMessage(DistributedAlg.NetworkMessage.newBuilder()
                        .setSenderListeningPort(port)
                        .setMessage(DistributedAlg.Message.newBuilder()
                                .setType(DistributedAlg.Message.Type.PROC_REGISTRATION)
                                .setToAbstractionId("app")
                                .setProcRegistration(DistributedAlg.ProcRegistration.newBuilder()
                                        .setOwner(owner)
                                        .setIndex(index)
                                        .build())
                                .build())
                        .build())
                .build();

        sendMessage(hubHost, hubPort, outer);
        logger.info("Registered to hub as {}-{} on {}:{}", owner, index, host, port);
    }

    private void startTcpListener() throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        ExecutorService pool = Executors.newFixedThreadPool(10);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            pool.submit(() -> handleClient(clientSocket));
        }
    }

    private void handleClient(Socket socket) {
        try (InputStream in = socket.getInputStream()) {
            DataInputStream dis = new DataInputStream(in);
            int size = dis.readInt();
            byte[] data = dis.readNBytes(size);
            DistributedAlg.Message msg = DistributedAlg.Message.parseFrom(data);
            handleMessage(msg);
        } catch (IOException e) {
            logger.error("Error handling client: {}", e.getMessage());
        }
    }

    private void handleMessage(DistributedAlg.Message msg) throws IOException {
        logger.info("{}-{} : Received message: {} from {} from abstraction: {} to abstraction: {}", this.owner, this.index, msg.getType(), msg.getSystemId(), msg.getFromAbstractionId(), msg.getToAbstractionId());
        DistributedAlg.Message innerMsg =
                (msg.getType() == DistributedAlg.Message.Type.NETWORK_MESSAGE)
                        ? msg.getNetworkMessage().getMessage()
                        : msg;
        switch (innerMsg.getType()) {
            case PROC_INITIALIZE_SYSTEM -> {
                var initMsg = innerMsg.getProcInitializeSystem();
                this.systemId = innerMsg.getSystemId();
                this.processes = new ArrayList<>(initMsg.getProcessesList());
                this.registerMap = new ConcurrentHashMap<>();
                this.readIdMap = new ConcurrentHashMap<>();
                this.readValuesMap = new ConcurrentHashMap<>();
                this.readTimestampsMap = new ConcurrentHashMap<>();
                this.readWriterRanksMap = new ConcurrentHashMap<>();
                this.ackCountMap = new ConcurrentHashMap<>();
                this.timestampMap = new ConcurrentHashMap<>();
                this.writeReadIdsMap = new ConcurrentHashMap<>();
                this.writeValueMap = new ConcurrentHashMap<>();
                logger.info("Initialized system: {}", systemId);
                processes.forEach(p -> logger.debug("- Process: {}-{} [{}:{}]",
                        p.getOwner(), p.getIndex(), p.getHost(), p.getPort()));
            }
            case BEB_DELIVER -> {
                var bebMsg = innerMsg.getBebDeliver();
                DistributedAlg.Message delivered = bebMsg.getMessage();
                DistributedAlg.ProcessId sender = bebMsg.getSender();

                logger.debug("BEB_DELIVER received from {}-{} for {}", sender.getOwner(), sender.getIndex(), delivered.getType());

                DistributedAlg.Message forwarded = DistributedAlg.Message.newBuilder()
                        .setType(delivered.getType())
                        .setSystemId(innerMsg.getSystemId())
                        .setFromAbstractionId(innerMsg.getFromAbstractionId())
                        .setToAbstractionId(innerMsg.getToAbstractionId())
                        .mergeFrom(delivered)
                        .build();

                handleMessage(forwarded);
            }
            case BEB_BROADCAST -> {
                DistributedAlg.Message inner = innerMsg.getBebBroadcast().getMessage();

                for (DistributedAlg.ProcessId pid : processes) {
                    if (!pid.getOwner().equals(this.owner) || pid.getIndex() != this.index) {
                        DistributedAlg.Message deliver = DistributedAlg.Message.newBuilder()
                                .setType(DistributedAlg.Message.Type.NETWORK_MESSAGE)
                                .setSystemId(innerMsg.getSystemId())
                                .setFromAbstractionId(innerMsg.getFromAbstractionId())
                                .setToAbstractionId(innerMsg.getToAbstractionId() + ".pl")
                                .setNetworkMessage(DistributedAlg.NetworkMessage.newBuilder()
                                        .setSenderHost(this.host)
                                        .setSenderListeningPort(this.port)
                                        .setMessage(inner)
                                        .build())
                                .build();

                        try {
                            sendMessage(pid.getHost(), pid.getPort(), deliver);
                        } catch (IOException e) {
                            logger.error("Failed to send broadcasted message to {}-{}: {}", pid.getOwner(), pid.getIndex(), e.getMessage());
                        }
                    }
                }
                DistributedAlg.Message localDeliver = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.BEB_DELIVER)
                        .setSystemId(innerMsg.getSystemId())
                        .setFromAbstractionId(innerMsg.getFromAbstractionId())
                        .setToAbstractionId(innerMsg.getToAbstractionId())
                        .setBebDeliver(DistributedAlg.BebDeliver.newBuilder()
                                .setMessage(inner)
                                .setSender(DistributedAlg.ProcessId.newBuilder()
                                        .setOwner(this.owner)
                                        .setIndex(this.index)
                                        .setHost(this.host)
                                        .setPort(this.port)
                                        .build())
                                .build())
                        .build();

                handleMessage(localDeliver);
            }
            case APP_BROADCAST -> {
                var value = innerMsg.getAppBroadcast().getValue();
                logger.info("Received AppBroadcast({}), sending AppValue to all...", value.getV());

                DistributedAlg.Message appValueMsg = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.APP_VALUE)
                        .setSystemId(innerMsg.getSystemId())
                        .setToAbstractionId("app")
                        .setAppValue(DistributedAlg.AppValue.newBuilder().setValue(value).build())
                        .build();

                broadcastToPeers(appValueMsg);
                sendMessage(hubHost, hubPort, wrapNetworkMessage(appValueMsg));
            }
            case APP_VALUE -> {
                int v = innerMsg.getAppValue().getValue().getV();
                logger.debug("Delivered value: {}", v);

                DistributedAlg.Message appValueMessage =
                                DistributedAlg.Message.newBuilder()
                                        .setType(DistributedAlg.Message.Type.APP_VALUE)
                                        .setSystemId(innerMsg.getSystemId())
                                        .setToAbstractionId("app")
                                        .setAppValue(DistributedAlg.AppValue.newBuilder().setValue(innerMsg.getAppValue().getValue()).build())
                                        .build();

                sendMessage(hubHost, hubPort, wrapNetworkMessage(appValueMessage));
            }
            case APP_PROPOSE -> {
                var topic = innerMsg.getAppPropose().getTopic();
                var value = innerMsg.getAppPropose().getValue();
                logger.info("Received AppPropose({}, {}), creating UC_PROPOSE.", topic, value.getV());

                                DistributedAlg.Message ucProposeMsg = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.UC_PROPOSE)
                        .setSystemId(innerMsg.getSystemId())
                        .setFromAbstractionId("app")
                        .setToAbstractionId("app.uc[" + topic + "]")
                        .setUcPropose(DistributedAlg.UcPropose.newBuilder().setValue(value).build())
                        .build();

                                handleMessage(ucProposeMsg);
            }
            case UC_PROPOSE -> {
                var value = innerMsg.getUcPropose().getValue();
                String topic = extractTopicFromAbstractionId(msg.getToAbstractionId());
                logger.info("Received UC_PROPOSE for topic '{}', value: {}, deciding immediately.", topic, value.getV());

                                DistributedAlg.Message ucDecideMsg = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.UC_DECIDE)
                        .setSystemId(innerMsg.getSystemId())
                        .setFromAbstractionId("app.uc[" + topic + "]")
                        .setToAbstractionId("app")
                        .setUcDecide(DistributedAlg.UcDecide.newBuilder().setValue(value).build())
                        .build();

                                handleMessage(ucDecideMsg);
            }
            case UC_DECIDE -> {
                var value = innerMsg.getUcDecide().getValue();
                String topic = extractTopicFromAbstractionId(innerMsg.getFromAbstractionId());
                logger.info("Received UC_DECIDE for topic '{}', value: {}, sending APP_DECIDE to hub.", topic, value.getV());

                                DistributedAlg.Message appDecideMsg = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.APP_DECIDE)
                        .setSystemId(innerMsg.getSystemId())
                        .setToAbstractionId("app")
                        .setAppDecide(DistributedAlg.AppDecide.newBuilder().setValue(value).build())
                        .build();

                sendMessage(hubHost, hubPort, wrapNetworkMessage(appDecideMsg));

                                                if (!topic.isEmpty()) {
                    logger.info("Sending APP_WRITE for register '{}', value: {} to hub.", topic, value.getV());

                                        DistributedAlg.Message appWriteMsg = DistributedAlg.Message.newBuilder()
                            .setType(DistributedAlg.Message.Type.APP_WRITE)
                            .setSystemId(innerMsg.getSystemId())
                            .setToAbstractionId("app")
                            .setAppWrite(DistributedAlg.AppWrite.newBuilder()
                                    .setRegister(topic)
                                    .setValue(value)
                                    .build())
                            .build();

                    sendMessage(hubHost, hubPort, wrapNetworkMessage(appWriteMsg));
                }
            }
            case APP_READ -> {
                String register = innerMsg.getAppRead().getRegister();
                logger.info("Read register '{}' request", register);

                                DistributedAlg.Value value = registerMap.getOrDefault(register, 
                        DistributedAlg.Value.newBuilder().setDefined(false).build());

                if (value.getDefined()) {
                    logger.info("Already have value for register '{}': {}", register, value.getV());

                                        DistributedAlg.Message appReadReturnMsg = DistributedAlg.Message.newBuilder()
                            .setType(DistributedAlg.Message.Type.APP_READ_RETURN)
                            .setSystemId(innerMsg.getSystemId())
                            .setToAbstractionId("app")
                            .setAppReadReturn(DistributedAlg.AppReadReturn.newBuilder()
                                    .setRegister(register)
                                    .setValue(value)
                                    .build())
                            .build();

                    sendMessage(hubHost, hubPort, wrapNetworkMessage(appReadReturnMsg));
                } else {
                                                            DistributedAlg.Message nnarReadMsg = DistributedAlg.Message.newBuilder()
                            .setType(DistributedAlg.Message.Type.NNAR_READ)
                            .setSystemId(innerMsg.getSystemId())
                            .setFromAbstractionId("app")
                            .setToAbstractionId("app.nnar[" + register + "]")
                            .setNnarRead(DistributedAlg.NnarRead.newBuilder().build())
                            .build();

                    handleMessage(nnarReadMsg);
                }
            }
            case APP_WRITE -> {
                String register = innerMsg.getAppWrite().getRegister();
                DistributedAlg.Value rawValue = innerMsg.getAppWrite().getValue();
                DistributedAlg.Value value = DistributedAlg.Value.newBuilder()
                        .setDefined(true)
                        .setV(rawValue.getV())
                        .build();
                logger.info("Write register '{}': {} request", register, value.getV());

                boolean isFromHub = msg.getFromAbstractionId().equals("app");
                if (isFromHub) {
                    logger.info("Broadcasting APP_WRITE for register '{}' to all peers", register);

                    DistributedAlg.Message appWriteMsg = DistributedAlg.Message.newBuilder()
                            .setType(DistributedAlg.Message.Type.APP_WRITE)
                            .setSystemId(innerMsg.getSystemId())
                            .setFromAbstractionId("app")
                            .setToAbstractionId("app")
                            .setAppWrite(innerMsg.getAppWrite())
                            .build();

                    DistributedAlg.Message bebBroadcast = DistributedAlg.Message.newBuilder()
                            .setType(DistributedAlg.Message.Type.BEB_BROADCAST)
                            .setSystemId(innerMsg.getSystemId())
                            .setFromAbstractionId("app.nnar[" + register + "]")
                            .setToAbstractionId("app.nnar[" + register + "].beb")
                            .setBebBroadcast(DistributedAlg.BebBroadcast.newBuilder()
                                    .setMessage(appWriteMsg)
                                    .build())
                            .build();

                    handleMessage(bebBroadcast);
                }

                DistributedAlg.Message nnarWriteMsg = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.NNAR_WRITE)
                        .setSystemId(innerMsg.getSystemId())
                        .setFromAbstractionId("app")
                        .setToAbstractionId("app.nnar[" + register + "]")
                        .setNnarWrite(DistributedAlg.NnarWrite.newBuilder()
                                .setValue(value)
                                .build())
                        .build();

                handleMessage(nnarWriteMsg);
            }
            case NNAR_READ -> {
                String register = extractRegisterFromAbstractionId(innerMsg.getToAbstractionId());

                logger.info("Starting NNAR_READ for register '{}'", register);

                if (!readIdMap.containsKey(register)) {
                    readIdMap.put(register, 0);
                }

                int readId = readIdMap.get(register);
                readIdMap.put(register, readId + 1);

                readValuesMap.putIfAbsent(register, new ConcurrentHashMap<>());
                readTimestampsMap.putIfAbsent(register, new ConcurrentHashMap<>());
                readWriterRanksMap.putIfAbsent(register, new ConcurrentHashMap<>());
                ackCountMap.putIfAbsent(register, new ConcurrentHashMap<>());

                DistributedAlg.Message internalReadMsg = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.NNAR_INTERNAL_READ)
                        .setSystemId(innerMsg.getSystemId())
                        .setFromAbstractionId("app.nnar[" + register + "]")
                        .setToAbstractionId("app.nnar[" + register + "]")
                        .setNnarInternalRead(DistributedAlg.NnarInternalRead.newBuilder()
                                .setReadId(readId)
                                .build())
                        .build();

                DistributedAlg.Message bebBroadcast = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.BEB_BROADCAST)
                        .setSystemId(innerMsg.getSystemId())
                        .setFromAbstractionId("app.nnar[" + register + "]")
                        .setToAbstractionId("app.nnar[" + register + "].beb")
                        .setBebBroadcast(DistributedAlg.BebBroadcast.newBuilder()
                                .setMessage(internalReadMsg)
                                .build())
                        .build();

                handleMessage(bebBroadcast);
            }
            case NNAR_INTERNAL_READ -> {
                String register = extractRegisterFromAbstractionId(innerMsg.getToAbstractionId());
                int readId = innerMsg.getNnarInternalRead().getReadId();

                DistributedAlg.Value value = registerMap.getOrDefault(register, 
                        DistributedAlg.Value.newBuilder().setDefined(false).build());
                int timestamp = timestampMap.getOrDefault(register, 0);
                int writerRank = this.index;

                logger.debug("NNAR_INTERNAL_READ for register '{}', readId: {}, local value: {}, timestamp: {}, writerRank: {}", 
                        register, readId, value.getV(), timestamp, writerRank);

                DistributedAlg.Message internalValueMsg = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.NNAR_INTERNAL_VALUE)
                        .setSystemId(innerMsg.getSystemId())
                        .setFromAbstractionId("app.nnar[" + register + "]")
                        .setToAbstractionId("app.nnar[" + register + "]")
                        .setNnarInternalValue(DistributedAlg.NnarInternalValue.newBuilder()
                                .setReadId(readId)
                                .setTimestamp(timestamp)
                                .setWriterRank(writerRank)
                                .setValue(value)
                                .build())
                        .build();

                if (msg.getType() == DistributedAlg.Message.Type.BEB_DELIVER) {
                    DistributedAlg.ProcessId sender = msg.getBebDeliver().getSender();
                    try {
                        sendMessage(sender.getHost(), sender.getPort(), wrapNetworkMessage(internalValueMsg, "app.nnar[" + register + "].beb.pl"));
                    } catch (IOException e) {
                        logger.error("Failed to send NNAR_INTERNAL_VALUE: {}", e.getMessage());
                    }
                } else {
                    handleMessage(internalValueMsg);
                }
            }
            case NNAR_INTERNAL_VALUE -> {
                String register = extractRegisterFromAbstractionId(innerMsg.getToAbstractionId());
                int readId = innerMsg.getNnarInternalValue().getReadId();
                int timestamp = innerMsg.getNnarInternalValue().getTimestamp();
                int writerRank = innerMsg.getNnarInternalValue().getWriterRank();
                DistributedAlg.Value value = innerMsg.getNnarInternalValue().getValue();

                logger.debug("Received NNAR_INTERNAL_VALUE for register '{}', readId: {}, timestamp: {}, writerRank: {}, value: {}", 
                        register, readId, timestamp, writerRank, value.getV());

                readValuesMap.get(register).put(readId, value);
                readTimestampsMap.get(register).put(readId, timestamp);
                readWriterRanksMap.get(register).put(readId, writerRank);

                ackCountMap.get(register).putIfAbsent(readId, 0);
                int ackCount = ackCountMap.get(register).get(readId) + 1;
                ackCountMap.get(register).put(readId, ackCount);

                if (ackCount > processes.size() / 2) {
                    logger.debug("Majority of processes responded for register '{}', readId: {}, ackCount: {}", 
                            register, readId, ackCount);

                    int highestTs = 0;
                    int highestRank = 0;
                    DistributedAlg.Value highestValue = DistributedAlg.Value.newBuilder().setDefined(false).build();

                    for (int ts : readTimestampsMap.get(register).values()) {
                        if (ts > highestTs) {
                            highestTs = ts;
                        }
                    }

                    logger.debug("Highest timestamp for register '{}': {}", register, highestTs);

                    for (Map.Entry<Integer, Integer> entry : readTimestampsMap.get(register).entrySet()) {
                        if (entry.getValue() == highestTs) {
                            int rank = readWriterRanksMap.get(register).get(entry.getKey());
                            if (rank > highestRank) {
                                highestRank = rank;
                                highestValue = readValuesMap.get(register).get(entry.getKey());
                                logger.debug("New highest rank for register '{}': {}, value: {}", 
                                        register, highestRank, highestValue.getV());
                            }
                        }
                    }

                                        registerMap.put(register, highestValue);
                    timestampMap.put(register, highestTs);

                    logger.info("Updated local value for register '{}': {}, timestamp: {}", 
                            register, highestValue.getV(), highestTs);

                    if (writeReadIdsMap.containsKey(register) && writeReadIdsMap.get(register).contains(readId)) {
                        DistributedAlg.Value originalWriteValue = writeValueMap.getOrDefault(register, new HashMap<>()).get(readId);
                        int myTs = timestampMap.getOrDefault(register, 0);

                        DistributedAlg.Value valueToWrite;
                        int newTimestamp;

                        if (myTs > highestTs || (myTs == highestTs && this.index > highestRank)) {
                            valueToWrite = originalWriteValue;
                            newTimestamp = myTs + 1;
                        } else {
                            valueToWrite = highestValue;
                            newTimestamp = highestTs + 1;
                        }
                        logger.info("This is a write operation for register '{}', readId: {}, valueToWrite: {}, newTimestamp: {}", 
                                register, readId, valueToWrite.getV(), newTimestamp);

                        DistributedAlg.Message internalWriteMsg = DistributedAlg.Message.newBuilder()
                                .setType(DistributedAlg.Message.Type.NNAR_INTERNAL_WRITE)
                                .setSystemId(msg.getSystemId())
                                .setFromAbstractionId("app.nnar[" + register + "]")
                                .setToAbstractionId("app.nnar[" + register + "]")
                                .setNnarInternalWrite(DistributedAlg.NnarInternalWrite.newBuilder()
                                        .setReadId(readId)
                                        .setTimestamp(newTimestamp)
                                        .setWriterRank(this.index)
                                        .setValue(valueToWrite)
                                        .build())
                                .build();

                        DistributedAlg.Message bebBroadcast = DistributedAlg.Message.newBuilder()
                                .setType(DistributedAlg.Message.Type.BEB_BROADCAST)
                                .setSystemId(msg.getSystemId())
                                .setFromAbstractionId("app.nnar[" + register + "]")
                                .setToAbstractionId("app.nnar[" + register + "].beb")
                                .setBebBroadcast(DistributedAlg.BebBroadcast.newBuilder()
                                        .setMessage(internalWriteMsg)
                                        .build())
                                .build();

                        handleMessage(bebBroadcast);
                        if (writeValueMap.containsKey(register)) {
                            writeValueMap.get(register).remove(readId);
                            if (writeValueMap.get(register).isEmpty()) {
                                writeValueMap.remove(register);
                            }
                        }
                    } else {
                                                logger.info("This is a read operation for register '{}', readId: {}, returning value: {}", 
                                register, readId, highestValue.getV());

                        DistributedAlg.Message readReturnMsg = DistributedAlg.Message.newBuilder()
                                .setType(DistributedAlg.Message.Type.NNAR_READ_RETURN)
                                .setSystemId(innerMsg.getSystemId())
                                .setFromAbstractionId("app.nnar[" + register + "]")
                                .setToAbstractionId("app")
                                .setNnarReadReturn(DistributedAlg.NnarReadReturn.newBuilder()
                                        .setValue(highestValue)
                                        .build())
                                .build();

                        handleMessage(readReturnMsg);
                    }

                    readValuesMap.get(register).remove(readId);
                    readTimestampsMap.get(register).remove(readId);
                    readWriterRanksMap.get(register).remove(readId);
                    ackCountMap.get(register).remove(readId);
                    Set<Integer> ids = writeReadIdsMap.get(register);
                    if (ids != null) {
                        ids.remove(readId);
                        if (ids.isEmpty()) {
                            writeReadIdsMap.remove(register);
                        }
                    }

                }
            }
            case NNAR_WRITE -> {
                String register = extractRegisterFromAbstractionId(innerMsg.getToAbstractionId());
                DistributedAlg.Value rawValue = innerMsg.getNnarWrite().getValue();
                DistributedAlg.Value value = DistributedAlg.Value.newBuilder()
                        .setDefined(true)
                        .setV(rawValue.getV())
                        .build();

                logger.info("Starting NNAR_WRITE for register '{}', value: {}", register, value.getV());

                if (!readIdMap.containsKey(register)) {
                    readIdMap.put(register, 0);
                }
                writeReadIdsMap.putIfAbsent(register, ConcurrentHashMap.newKeySet());

                int readId = readIdMap.get(register);
                readIdMap.put(register, readId + 1);

                writeReadIdsMap.get(register).add(readId);

                readValuesMap.putIfAbsent(register, new ConcurrentHashMap<>());
                readTimestampsMap.putIfAbsent(register, new ConcurrentHashMap<>());
                readWriterRanksMap.putIfAbsent(register, new ConcurrentHashMap<>());
                ackCountMap.putIfAbsent(register, new ConcurrentHashMap<>());

                readValuesMap.get(register).put(readId, value);
                writeValueMap.putIfAbsent(register, new ConcurrentHashMap<>());

                writeValueMap.get(register).put(readId, value);

                DistributedAlg.Message internalReadMsg = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.NNAR_INTERNAL_READ)
                        .setSystemId(innerMsg.getSystemId())
                        .setFromAbstractionId("app.nnar[" + register + "]")
                        .setToAbstractionId("app.nnar[" + register + "]")
                        .setNnarInternalRead(DistributedAlg.NnarInternalRead.newBuilder()
                                .setReadId(readId)
                                .build())
                        .build();

                DistributedAlg.Message bebBroadcast = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.BEB_BROADCAST)
                        .setSystemId(innerMsg.getSystemId())
                        .setFromAbstractionId("app.nnar[" + register + "]")
                        .setToAbstractionId("app.nnar[" + register + "].beb")
                        .setBebBroadcast(DistributedAlg.BebBroadcast.newBuilder()
                                .setMessage(internalReadMsg)
                                .build())
                        .build();

                handleMessage(bebBroadcast);
            }
            case NNAR_INTERNAL_WRITE -> {
                String register = extractRegisterFromAbstractionId(innerMsg.getToAbstractionId());
                int readId = innerMsg.getNnarInternalWrite().getReadId();
                int timestamp = innerMsg.getNnarInternalWrite().getTimestamp();
                DistributedAlg.Value value = innerMsg.getNnarInternalWrite().getValue();

                logger.info("Writing register '{}', readId: {}, timestamp: {}, value: {}", 
                        register, readId, timestamp, value.getV());

                                registerMap.put(register, value);
                timestampMap.put(register, timestamp);

                                DistributedAlg.Message ackMsg = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.NNAR_INTERNAL_ACK)
                        .setSystemId(msg.getSystemId())
                        .setFromAbstractionId("app.nnar[" + register + "]")
                        .setToAbstractionId("app.nnar[" + register + "]")
                        .setNnarInternalAck(DistributedAlg.NnarInternalAck.newBuilder()
                                .setReadId(readId)
                                .build())
                        .build();
                if (msg.getType() == DistributedAlg.Message.Type.NETWORK_MESSAGE &&
                        !msg.getNetworkMessage().getSenderHost().isEmpty() &&
                        msg.getNetworkMessage().getSenderListeningPort() > 0) {
                    String senderHost = msg.getNetworkMessage().getSenderHost();
                    int senderPort = msg.getNetworkMessage().getSenderListeningPort();
                    try {
                        sendMessage(senderHost, senderPort, wrapNetworkMessage(ackMsg, "app.nnar[" + register + "].beb.pl"));
                    } catch (IOException e) {
                        logger.error("Failed to send NNAR_INTERNAL_ACK: {}", e.getMessage());
                    }
                } else {
                    handleMessage(ackMsg);
                }
            }
            case NNAR_INTERNAL_ACK -> {
                                String register = extractRegisterFromAbstractionId(innerMsg.getToAbstractionId());
                int readId = innerMsg.getNnarInternalAck().getReadId();

                                ackCountMap.get(register).putIfAbsent(readId, 0);
                int ackCount = ackCountMap.get(register).get(readId) + 1;
                ackCountMap.get(register).put(readId, ackCount);

                                if (ackCount > processes.size() / 2) {
                                        DistributedAlg.Message writeReturnMsg = DistributedAlg.Message.newBuilder()
                            .setType(DistributedAlg.Message.Type.NNAR_WRITE_RETURN)
                            .setSystemId(msg.getSystemId())
                            .setFromAbstractionId("app.nnar[" + register + "]")
                            .setToAbstractionId("app")
                            .setNnarWriteReturn(DistributedAlg.NnarWriteReturn.newBuilder().build())
                            .build();

                    handleMessage(writeReturnMsg);

                                        ackCountMap.get(register).remove(readId);
                }
            }
            case NNAR_READ_RETURN -> {
                String register = extractRegisterFromAbstractionId(innerMsg.getFromAbstractionId());
                DistributedAlg.Value value = innerMsg.getNnarReadReturn().getValue();

                                logger.info("Read register '{}' value: {}", register, value.getV());

                                DistributedAlg.Message appReadReturnMsg = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.APP_READ_RETURN)
                        .setSystemId(innerMsg.getSystemId())
                        .setToAbstractionId("app")
                        .setAppReadReturn(DistributedAlg.AppReadReturn.newBuilder()
                                .setRegister(register)
                                .setValue(value)
                                .build())
                        .build();

                sendMessage(hubHost, hubPort, wrapNetworkMessage(appReadReturnMsg));
            }
            case NNAR_WRITE_RETURN -> {
                String register = extractRegisterFromAbstractionId(msg.getFromAbstractionId());

                logger.info("Write completed for register '{}'", register);

                                DistributedAlg.Message appWriteReturnMsg = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.APP_WRITE_RETURN)
                        .setSystemId(innerMsg.getSystemId())
                        .setToAbstractionId("app")
                        .setAppWriteReturn(DistributedAlg.AppWriteReturn.newBuilder()
                                .setRegister(register)
                                .build())
                        .build();

                sendMessage(hubHost, hubPort, wrapNetworkMessage(appWriteReturnMsg));
            }
            case PROC_DESTROY_SYSTEM -> {
                logger.info("System destroyed: {}", innerMsg.getSystemId());
                this.systemId = null;
                this.processes.clear();
                this.registerMap.clear();
                this.readIdMap.clear();
                this.readValuesMap.clear();
                this.readTimestampsMap.clear();
                this.readWriterRanksMap.clear();
                this.ackCountMap.clear();
                this.timestampMap.clear();
            }
            default -> logger.warn("Unhandled message type: {}", innerMsg.getType());
        }
    }

    private void broadcastToPeers(DistributedAlg.Message innerMsg) {
        broadcastToPeers(innerMsg, "app.beb.pl");
    }

    private void broadcastToPeers(DistributedAlg.Message innerMsg, String abstractionId) {
        for (DistributedAlg.ProcessId pid : processes) {
            if (!pid.getOwner().equals(this.owner) || pid.getIndex() != this.index) {
                DistributedAlg.Message wrapper = wrapNetworkMessage(innerMsg, abstractionId);
                try {
                    sendMessage(pid.getHost(), pid.getPort(), wrapper);
                } catch (IOException e) {
                    logger.error("Failed to send message to {}-{}: {}", pid.getOwner(), pid.getIndex(), e.getMessage());
                }
            }
        }
    }

    private DistributedAlg.Message wrapNetworkMessage(DistributedAlg.Message msg) {
       return wrapNetworkMessage(msg, "app");
    }

    private DistributedAlg.Message wrapNetworkMessage(DistributedAlg.Message msg, String abstractionId) {
        return DistributedAlg.Message.newBuilder()
                .setType(DistributedAlg.Message.Type.NETWORK_MESSAGE)
                .setSystemId(systemId)
                .setToAbstractionId(abstractionId)
                .setNetworkMessage(DistributedAlg.NetworkMessage.newBuilder()
                        .setSenderHost(this.host)
                        .setSenderListeningPort(this.port)
                        .setMessage(msg)
                        .build())
                .build();
    }

    private void sendMessage(String host, int port, DistributedAlg.Message message) throws IOException {
        int maxRetries = 3;
        int retryDelayMs = 100;
        IOException lastException = null;

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                Socket socket = new Socket();
                socket.setReuseAddress(true);
                socket.setSoTimeout(2000);                 socket.connect(new InetSocketAddress(host, port), 2000);

                try (OutputStream out = socket.getOutputStream()) {
                    byte[] payload = message.toByteArray();
                    DataOutputStream dos = new DataOutputStream(out);
                    dos.writeInt(payload.length);
                    dos.write(payload);
                    dos.flush();
                } finally {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        logger.warn("Error closing socket: {}", e.getMessage());
                    }
                }
                return;             } catch (IOException e) {
                lastException = e;
                logger.warn("Attempt {} failed to send message to {}:{}: {}", attempt + 1, host, port, e.getMessage());

                if (attempt < maxRetries - 1) {
                    try {
                                                Thread.sleep(retryDelayMs * (1 << attempt));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted during retry delay", ie);
                    }
                }
            }
        }

                logger.error("Failed to send message after {} attempts", maxRetries);
        throw lastException;
    }

    private String extractRegisterFromAbstractionId(String abstractionId) {
        if (abstractionId.contains("app.nnar[") && abstractionId.contains("]")) {
            return abstractionId.substring(abstractionId.indexOf("[") + 1, abstractionId.indexOf("]"));
        }
        return "";
    }

    private String extractTopicFromAbstractionId(String abstractionId) {
        if (abstractionId.contains("app.uc[") && abstractionId.contains("]")) {
            return abstractionId.substring(abstractionId.indexOf("[") + 1, abstractionId.indexOf("]"));
        }
        return "";
    }
}
