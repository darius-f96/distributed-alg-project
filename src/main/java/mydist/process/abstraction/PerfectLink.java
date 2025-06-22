package mydist.process.abstraction;

import mydist.datastructures.distributed.DistributedAlg;
import mydist.datastructures.distributed.DistributedAlg.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class PerfectLink implements AbstractionLayer{

    private static final Logger logger = LoggerFactory.getLogger(PerfectLink.class);
    private final BlockingQueue<Message> messageQ;
    private final List<ProcessId> processes;
    private final String hubHost;
    private final int hubPort;
    private String parentAbstractionId;
    private final String systemId;
    private final String host;
    private final int port;

    public PerfectLink(BlockingQueue<Message> messageQ, List<ProcessId> processes, String host, int port, String hubHost, int hubPort, String systemId) {
        this.messageQ = messageQ;
        this.processes = processes;
        this.host = host;
        this.port = port;
        this.hubHost = hubHost;
        this.hubPort = hubPort;
        this.systemId = systemId;
    }

    public PerfectLink createCopyWithParentAbstractionId(String parentAbstractionId) {
        PerfectLink copy = new PerfectLink(messageQ, processes, host, port, hubHost, hubPort, systemId);
        copy.parentAbstractionId = parentAbstractionId;
        return copy;
    }

    @Override
    public void handleMessage(Message msg) {
        switch (msg.getType()) {
            case NETWORK_MESSAGE -> {
                NetworkMessage networkMessage = msg.getNetworkMessage();

                ProcessId sender = processes.stream()
                        .filter(p -> p.getHost().equals(networkMessage.getSenderHost()) &&
                                p.getPort() == networkMessage.getSenderListeningPort())
                        .findFirst()
                        .orElse(null);

                if (sender == null) {
                    logger.warn("No sender found for host {} and port: {}", networkMessage.getSenderHost(), networkMessage.getSenderListeningPort());
                }

                PlDeliver.Builder plDeliver = PlDeliver.newBuilder()
                        .setMessage(networkMessage.getMessage());
                if (sender != null)
                    plDeliver.setSender(sender);

                Message outgoingMessage = Message.newBuilder()
                        .setType(Message.Type.PL_DELIVER)
                        .setSystemId(msg.getSystemId())
                        .setFromAbstractionId(msg.getToAbstractionId())
                        .setToAbstractionId(parentAbstractionId)
                        .setPlDeliver(plDeliver.build())
                        .build();

                messageQ.offer(outgoingMessage);
            }
            case PL_SEND -> {
                try{
                    sendMessage(msg);
                }catch (Exception e){
                    logger.warn("Exception stack trace", e);
                    logger.error("Caught exception : {}", e.getMessage());
                }
            }
        }
    }

    private void sendMessage(Message message) throws IOException {
        int maxRetries = 3;
        int retryDelayMs = 100;
        IOException lastException = null;

        Message outgoingMessage = wrapNetworkMessage(message);
        String destHost;
        int destPort;

        if (message.getPlSend().hasDestination()){
            destHost = message.getPlSend().getDestination().getHost();
            destPort = message.getPlSend().getDestination().getPort();
        }
        else {
            destHost = hubHost;
            destPort = hubPort;
        }

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                Socket socket = new Socket();
                socket.setReuseAddress(true);
                socket.setSoTimeout(2000);
                socket.connect(new InetSocketAddress(destHost, destPort), 2000);
                try (OutputStream out = socket.getOutputStream()) {
                    byte[] payload = outgoingMessage.toByteArray();
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
                return;
            } catch (IOException e) {
                lastException = e;
                logger.warn("Attempt {} failed to send message to {}:{}: {}", attempt + 1, destHost, destPort, e.getMessage());

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

    private Message wrapNetworkMessage(Message msg) {
        return DistributedAlg.Message.newBuilder()
                .setType(DistributedAlg.Message.Type.NETWORK_MESSAGE)
                .setSystemId(systemId)
                .setToAbstractionId(msg.getToAbstractionId())
                .setNetworkMessage(DistributedAlg.NetworkMessage.newBuilder()
                        .setSenderHost(this.host)
                        .setSenderListeningPort(this.port)
                        .setMessage(msg.getPlSend().getMessage())
                        .build())
                .build();
    }

    @Override
    public void cleanup() {

    }

}
