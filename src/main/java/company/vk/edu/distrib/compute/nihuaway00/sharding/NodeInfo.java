package company.vk.edu.distrib.compute.nihuaway00.sharding;

public class NodeInfo {
    private final String endpoint;
    private boolean alive;

    public NodeInfo(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public boolean isEnabled() {
        return alive;
    }

    public void enable() {
        this.alive = true;
    }

    public void disable() {
        this.alive = false;
    }
}
