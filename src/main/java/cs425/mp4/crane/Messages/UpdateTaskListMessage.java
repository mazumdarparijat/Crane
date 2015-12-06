package cs425.mp4.crane.Messages;


/**
 * Created by parijatmazumdar on 03/12/15.
 */
public class UpdateTaskListMessage implements Message {
    public static final String messageType="UTL";
    public String getMessageType() {
        return messageType;
    }

    public String getMessagePayload() {
        return null;
    }
}
