package cs425.mp4.crane;

import cs425.mp4.crane.Topology.Bolt;

/**
 * (Bolt,Forwarder) pair. Task contains necessary info needed to
 * launch bolt tasks in worker
 */
public class Task {
    public final Bolt b;
    public final Forwarder fd;

    public Task(Bolt b, Forwarder fd) {
        this.b = b;
        this.fd = fd;
    }
}
