package cs425.mp4.crane;

import java.io.Serializable;

/**
 * Created by parijatmazumdar on 02/12/15.
 */
public class TaskAddress implements Serializable {
    static final long serialVersionUID = 70L;
    public final String hostname;
    public final int port;

    public TaskAddress(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }
}
