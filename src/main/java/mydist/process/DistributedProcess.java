package mydist.process;

import mydist.datastructures.distributed.DistributedAlg;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;


public class DistributedProcess {
    private final String owner;
    private final int index;
    private final String host;
    private final int port;
    private final String hubHost;
    private final int hubPort;
    private String systemId = "";
    private List<DistributedAlg.ProcessId> processes;
    private Map<String, DistributedAlg.Value> registerMap;

    public DistributedProcess(String owner, int index, String host, int port, String hubHost, int hubPort) {
        this.owner = owner;
        this.index = index;
        this.host = host;
        this.port = port;
        this.hubHost = hubHost;
        this.hubPort = hubPort;
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
        System.out.printf("Registered to hub as %s-%d on %s:%d\n", owner, index, host, port);
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
            System.err.println("Error handling client: " + e.getMessage());
        }
    }

    private void handleMessage(DistributedAlg.Message msg) throws IOException {
        System.out.printf("%s-%d : Received message: %s from %s from abstraction: %s to abstraction: %s\n", this.owner, this.index, msg.getType(), msg.getSystemId(), msg.getFromAbstractionId(), msg.getToAbstractionId());

        switch (msg.getType()) {
            case NETWORK_MESSAGE -> {
                DistributedAlg.NetworkMessage netMsg = msg.getNetworkMessage();
                DistributedAlg.Message inner = netMsg.getMessage();
                handleMessage(inner);
            }
            case PROC_INITIALIZE_SYSTEM -> {
                var initMsg = msg.getProcInitializeSystem();
                this.systemId = msg.getSystemId();
                this.processes = new ArrayList<>(initMsg.getProcessesList());
                System.out.println("Initialized system: " + systemId);
                processes.forEach(p -> System.out.printf("- Process: %s-%d [%s:%d]\n",
                        p.getOwner(), p.getIndex(), p.getHost(), p.getPort()));
            }
            case APP_BROADCAST -> {
                var value = msg.getAppBroadcast().getValue();
                System.out.printf("Received AppBroadcast(%d), sending AppValue to all...\n", value.getV());

                DistributedAlg.Message appValueMsg = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.APP_VALUE)
                        .setSystemId(msg.getSystemId())
                        .setToAbstractionId("app")
                        .setAppValue(DistributedAlg.AppValue.newBuilder().setValue(value).build())
                        .build();

                broadcastToPeers(appValueMsg);
            }
            case APP_VALUE -> {
                int v = msg.getAppValue().getValue().getV();
                System.out.printf("Delivered value: %d\n", v);

                DistributedAlg.Message appValueMessage =
                                DistributedAlg.Message.newBuilder()
                                        .setType(DistributedAlg.Message.Type.APP_VALUE)
                                        .setSystemId(msg.getSystemId())
                                        .setToAbstractionId("app")
                                        .setAppValue(DistributedAlg.AppValue.newBuilder().setValue(msg.getAppValue().getValue()).build())
                                        .build();


                sendMessage(hubHost, hubPort, wrapNetworkMessage(appValueMessage));
            }
            case APP_PROPOSE -> {
                var topic = msg.getAppPropose().getTopic();
                var value = msg.getAppPropose().getValue();
                System.out.printf("Received AppPropose(%s, %d), will decide immediately.\n", topic, value.getV());

                DistributedAlg.Message appDecideMsg = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.APP_DECIDE)
                        .setSystemId(msg.getSystemId())
                        .setToAbstractionId("app")
                        .setAppDecide(DistributedAlg.AppDecide.newBuilder().setValue(value).build())
                        .build();

                sendMessage(hubHost, hubPort, wrapNetworkMessage(appDecideMsg));
            }
            case APP_READ -> {
                String register = msg.getAppRead().getRegister();
                DistributedAlg.Value readVal = registerMap.getOrDefault(register,
                        DistributedAlg.Value.newBuilder().setDefined(false).build());
                System.out.printf("Read register '%s': %s\n", register, readVal);

                DistributedAlg.Message reply = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.APP_READ_RETURN)
                        .setSystemId(msg.getSystemId())
                        .setToAbstractionId("app")
                        .setAppReadReturn(DistributedAlg.AppReadReturn.newBuilder()
                                .setRegister(register)
                                .setValue(readVal)
                                .build())
                        .build();
                sendMessage(hubHost, hubPort, wrapNetworkMessage(reply));
            }
            case APP_WRITE -> {
                String register = msg.getAppWrite().getRegister();
                DistributedAlg.Value value = msg.getAppWrite().getValue();
                registerMap.put(register, value);
                System.out.printf("Write register '%s': %d\n", register, value.getV());

                DistributedAlg.Message ack = DistributedAlg.Message.newBuilder()
                        .setType(DistributedAlg.Message.Type.APP_WRITE_RETURN)
                        .setSystemId(msg.getSystemId())
                        .setToAbstractionId("app")
                        .setAppWriteReturn(DistributedAlg.AppWriteReturn.newBuilder()
                                .setRegister(register)
                                .build())
                        .build();
                sendMessage(hubHost, hubPort, wrapNetworkMessage(ack));
            }
            case PROC_DESTROY_SYSTEM -> {
                System.out.println("System destroyed: " + msg.getSystemId());
                this.systemId = null;
                this.processes.clear();
                this.registerMap.clear();
            }
            default -> System.out.printf("Unhandled message type: %s\n", msg.getType());
        }
    }

    private void broadcastToPeers(DistributedAlg.Message innerMsg) {
        for (DistributedAlg.ProcessId pid : processes) {
            if (!pid.getOwner().equals(this.owner) || pid.getIndex() != this.index) {
                DistributedAlg.Message wrapper = wrapNetworkMessage(innerMsg, "app.beb.pl");
                try {
                    sendMessage(pid.getHost(), pid.getPort(), wrapper);
                } catch (IOException e) {
                    System.err.printf("Failed to send message to %s-%d: %s\n", pid.getOwner(), pid.getIndex(), e.getMessage());
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
                        .setSenderListeningPort(this.port)
                        .setMessage(msg)
                        .build())
                .build();
    }

    private void sendMessage(String host, int port, DistributedAlg.Message message) throws IOException {
        try (Socket socket = new Socket(host, port);
             OutputStream out = socket.getOutputStream()) {
            byte[] payload = message.toByteArray();
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeInt(payload.length);
            dos.write(payload);
        }
    }
}
