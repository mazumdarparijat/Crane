package cs425.mp4.crane.Messages;

/**
 * Okay Message
 */
public class OkayMessage implements Message {
    public static final String messageType="OK";
    public String getMessageType() {
        return messageType;
    }

    public String getMessagePayload() {
        return null;
    }
}
