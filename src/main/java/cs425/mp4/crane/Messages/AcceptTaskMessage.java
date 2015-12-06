package cs425.mp4.crane.Messages;

/**
 * Created by parijatmazumdar on 02/12/15.
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
