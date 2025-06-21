package mydist.process.abstraction;

import mydist.datastructures.distributed.DistributedAlg;

import java.util.HashMap;
import java.util.Map;

public class AbstractionData {
    private Map<String, DistributedAlg.Value> register;

    public AbstractionData() {
        register = new HashMap<>();
    }

    public void writeRegister(String key, DistributedAlg.Value value) {
        register.put(key, value);
    }

    public DistributedAlg.Value readRegister(String key) {
        return register.getOrDefault(key,
                DistributedAlg.Value.newBuilder().setDefined(false).build());
    }
}
