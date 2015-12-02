package Topology;

import java.io.Serializable;

/**
 * Created by parijatmazumdar on 01/12/15.
 */
public interface Bolt extends Serializable {
    static final long serialVersionUID = 75L;
    public abstract void open();
    public abstract void nextTuple();
}
