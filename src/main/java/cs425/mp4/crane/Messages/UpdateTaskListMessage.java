package cs425.mp4.crane.Messages;


/**
 * Update addresses of tasks request. Nimbus -> worker
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
