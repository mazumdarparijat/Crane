package cs425.mp4.crane.Messages;

/**
 * Accept task request. Nimbus -> Worker
 */
public class AcceptTaskMessage implements Message {
    public static final String messageType="ATA";
    public String getMessageType() {
        return messageType;
    }

    public String getMessagePayload() {
        return null;
    }
}
