package cs425.mp4.crane.Topology;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Spout interface
 */
public interface Spout extends Serializable {
    static final long serialVersionUID = 75L;
    public abstract void open();
    public abstract HashMap<String,String> nextTuple();
}
