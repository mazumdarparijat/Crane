package cs425.mp4.crane;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by parijatmazumdar on 03/12/15.
 */
public class CraneData implements Serializable {
    private static final long serialVersionUID = 70L;
    public final long tupleID;
    public HashMap<String,String> val;

    public CraneData(long tupleID, HashMap<String, String> val) {
        this.tupleID = tupleID;
        this.val = val;
    }
}
