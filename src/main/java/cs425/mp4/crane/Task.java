package cs425.mp4.crane;

import cs425.mp4.crane.Topology.Bolt;

/**
 * Created by parijatmazumdar on 02/12/15.
 */
public class Task {
    public final Bolt b;
    public final Forwarder fd;

    public Task(Bolt b, Forwarder fd) {
        this.b = b;
        this.fd = fd;
    }
}
