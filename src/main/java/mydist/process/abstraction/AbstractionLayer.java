package mydist.process.abstraction;

import mydist.datastructures.distributed.DistributedAlg;

public interface AbstractionLayer {

    void handleMessage(DistributedAlg.Message msg);
    void cleanup();
}
