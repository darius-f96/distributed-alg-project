package mydist;

import mydist.process.DistributedProcess;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws IOException {
        if (args.length < 4) {
            System.err.println("Usage: <hubHost> <hubPort> <nodeHost> <port1> [<port2> <port3>]");
            System.exit(1);
        }

        ExecutorService executor = Executors.newFixedThreadPool(3);

        String hubHost = args[0];
        int hubPort = Integer.parseInt(args[1]);
        String nodeHost = args[2];

        for (int i = 3; i < args.length; i++) {
            int port = Integer.parseInt(args[i]);
            int index = i - 2;

            String owner = "abc";
            System.out.printf("Starting node %s-%d at %s:%d, connecting to hub at %s:%d\n",
                    owner, index, nodeHost, port, hubHost, hubPort);

            DistributedProcess proc = new DistributedProcess(
                    owner,
                    index,
                    nodeHost,
                    port,
                    hubHost,
                    hubPort
            );
            executor.submit(proc::start);
        }
    }
}