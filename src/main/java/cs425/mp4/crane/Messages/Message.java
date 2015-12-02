package cs425.mp4.crane.Messages;

import java.io.Serializable;

/**
 * Created by parijatmazumdar on 01/12/15.
 */
public interface Message extends Serializable {
    static final long serialVersionUID = 76L;
    public abstract String getMessageType();
    public abstract String getMessagePayload();
}
