package mydist.process.abstraction.consensus;

import mydist.datastructures.distributed.DistributedAlg;

public record EpState(int ets, DistributedAlg.Value value) {}
