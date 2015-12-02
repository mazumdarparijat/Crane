package Messages;

/**
 * Created by parijatmazumdar on 01/12/15.
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
