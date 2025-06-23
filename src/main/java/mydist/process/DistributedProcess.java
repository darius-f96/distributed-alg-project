package mydist.process;

import mydist.datastructures.distributed.DistributedAlg;
import mydist.process.abstraction.*;
import mydist.process.abstraction.consensus.EpochChange;
import mydist.process.abstraction.consensus.EpochLeaderDetector;
import mydist.process.abstraction.consensus.EventuallyPerfectFailureDetector;
import mydist.process.abstraction.consensus.UniformConsensus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class DistributedProcess {
    private static final Logger logger = LoggerFactory.getLogger(DistributedProcess.class);
    private final BlockingQueue<DistributedAlg.Message> messageQueue;
    private final Map<String, AbstractionLayer> abstractions = new HashMap<>();
    private final String owner;
    private final int index;
    private final String host;
    private final int port;
    private final String hubHost;
    private final int hubPort;
    private String systemId = "";
    private List<DistributedAlg.ProcessId> processes;
    private DistributedAlg.ProcessId currentProcess;
    private Thread processQueueThread;

    public DistributedProcess(String owner, int index, String host, int port, String hubHost, int hubPort) {
        this.owner = owner;
        this.index = index;
        this.host = host;
        this.port = port;
        this.hubHost = hubHost;
        this.hubPort = hubPort;
        this.messageQueue = new LinkedBlockingQueue<>();
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
    private void registerAbstractions() {
        PerfectLink pl = new PerfectLink(
                messageQueue,
                processes,
                host,
                port,
                hubHost,
                hubPort,
                systemId
        );
        abstractions.put("app", new App(messageQueue));

        abstractions.put("app.pl", pl.createCopyWithParentAbstractionId("app"));

        abstractions.put("app.beb",
                new BestEffortBroadcast(messageQueue, processes, "app.beb"));

        abstractions.put("app.beb.pl", pl.createCopyWithParentAbstractionId("app.beb"));
    }

    private void startTcpListener() throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        while (true) {
            Socket clientSocket = serverSocket.accept();
            handleClient(clientSocket);
        }
    }

    private void handleClient(Socket socket) {
        try (InputStream in = socket.getInputStream()) {
            DataInputStream dis = new DataInputStream(in);
            int size = dis.readInt();
            byte[] data = dis.readNBytes(size);
            DistributedAlg.Message msg = DistributedAlg.Message.parseFrom(data);
            if (isInitMessage(msg)) {
                handleMessage(msg);
            } else {
                messageQueue.offer(msg);
            }
        } catch (IOException e) {
            logger.error("Error handling client: {}", e.getMessage());
        }
    }

    private void startProcessingQueue() {
        processQueueThread = new Thread(() -> {
            try {
                while (true) {
                    DistributedAlg.Message msg = messageQueue.take();
                    handleMessage(msg);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Process queue interrupted, shutting down.");
            } catch (IOException e) {
                logger.error("Error while processing message queue: {}", e.getMessage());
            }
        });
        processQueueThread.setName("message-processing-thread-" + index);
        processQueueThread.start();
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
                logger.info("Initialized system: {}", systemId);
                processes.forEach(p -> logger.debug("- Process: {}-{} [{}:{}]",
                        p.getOwner(), p.getIndex(), p.getHost(), p.getPort()));
                currentProcess = processes.stream().filter(p-> p.getIndex() == index && p.getOwner().equals(owner)).findFirst().orElseThrow(RuntimeException::new);
                registerAbstractions();
                startProcessingQueue();
            }
            case PROC_DESTROY_SYSTEM -> {
                logger.info("System destroyed: {}", innerMsg.getSystemId());
                cleanup();
            }
            default -> {
                String toAbstraction = msg.getToAbstractionId();
                if (!abstractions.containsKey(toAbstraction) && toAbstraction.startsWith("app.nnar[")) {
                    String key = extractRegisterFromAbstractionId(toAbstraction);
                    logger.info("{}-{} : Registering new abstraction for {}", owner, index, toAbstraction);
                    registerNnarAbstractions(key);
                } else if (!abstractions.containsKey(toAbstraction) && toAbstraction.startsWith("app.uc[")) {
                    String topic = extractTopicFromAbstractionId(toAbstraction);
                    logger.info("{}-{} : Registering new topic for {}", owner, index, toAbstraction);
                    registerConseusAbstractions(topic);
                }
                AbstractionLayer handler = abstractions.get(toAbstraction);
                if (handler != null)
                    handler.handleMessage(msg);
                else
                    logger.error("{}-{}: No handler defined for {}", owner, index, toAbstraction);

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
    private void registerNnarAbstractions(String key) {
        String abstractionId = "app.nnar[" + key + "]";

        PerfectLink pl = new PerfectLink(
                messageQueue,
                processes,
                host,
                port,
                hubHost,
                hubPort,
                systemId
        );

        abstractions.put(abstractionId, new NNAtomicRegister(messageQueue, key, this.index, processes.size(), owner));

        abstractions.put(abstractionId + ".pl", pl.createCopyWithParentAbstractionId(abstractionId));

        abstractions.put(abstractionId + ".beb",
                new BestEffortBroadcast(messageQueue, processes, abstractionId + ".beb"));

        abstractions.put(abstractionId + ".beb.pl", pl.createCopyWithParentAbstractionId(abstractionId + ".beb"));
    }

    private void registerConseusAbstractions(String topic) {
        String abstractionId = "app.uc[" + topic + "]";
        PerfectLink pl = new PerfectLink(
                messageQueue,
                processes,
                host,
                port,
                hubHost,
                hubPort,
                systemId
        );

        abstractions.put(abstractionId, new UniformConsensus(abstractionId, messageQueue, abstractions, processes, owner, index, pl));
        abstractions.put(abstractionId + ".ec", new EpochChange(abstractionId, abstractionId + ".ec", currentProcess, messageQueue, processes));
        abstractions.put(abstractionId + ".ec.pl", pl.createCopyWithParentAbstractionId(abstractionId + ".ec"));
        abstractions.put(abstractionId + ".ec.beb", new BestEffortBroadcast(messageQueue, processes, abstractionId + ".ec.beb"));
        abstractions.put(abstractionId + ".ec.beb.pl", pl.createCopyWithParentAbstractionId(abstractionId + ".ec.beb"));
        abstractions.put(abstractionId + ".ec.eld", new EpochLeaderDetector(messageQueue, abstractionId + ".ec", abstractionId + ".ec.eld", processes));
        abstractions.put(abstractionId + ".ec.eld.epfd", new EventuallyPerfectFailureDetector(abstractionId + ".ec.eld", abstractionId + ".ec.eld.epfd", messageQueue, processes ));
        abstractions.put(abstractionId + ".ec.eld.epfd.pl", pl.createCopyWithParentAbstractionId(abstractionId + ".ec.eld.epfd"));

    }
    private boolean isInitMessage(DistributedAlg.Message msg) {
        if (msg.getType() == DistributedAlg.Message.Type.NETWORK_MESSAGE &&
                msg.getNetworkMessage().hasMessage()) {
            return msg.getNetworkMessage().getMessage().getType() ==
                    DistributedAlg.Message.Type.PROC_INITIALIZE_SYSTEM;
        }
        return false;
    }

    public void cleanup() {
        logger.info("Cleaning up DistributedProcess");
        if (processQueueThread != null) {
            processQueueThread.interrupt();
        }
        abstractions.values().forEach(AbstractionLayer::cleanup);
        abstractions.clear();
        processes = null;
        systemId = "";
    }
}
