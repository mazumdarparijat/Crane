package cs425.mp4.crane.Messages;

import java.io.Serializable;

/**
 * Message interface
 */
public interface Message extends Serializable {
    static final long serialVersionUID = 76L;
    public abstract String getMessageType();
    public abstract String getMessagePayload();
}
