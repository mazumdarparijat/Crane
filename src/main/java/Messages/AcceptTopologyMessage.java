package Messages;

/**
 * Created by parijatmazumdar on 01/12/15.
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
