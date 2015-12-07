package cs425.mp4.crane.Messages;

/**
 * Accept topology request. CraneSubmitter -> nimbus
 */
public class AcceptTopologyMessage implements Message {
    public static final String messageType="AT";
    public String getMessageType() {
        return messageType;
    }

    public String getMessagePayload() {
        return null;
    }
}
