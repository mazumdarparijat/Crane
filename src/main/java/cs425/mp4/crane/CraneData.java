package cs425.mp4.crane;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Data structure used to communicate results between spout and
 * bolt tasks. Its essentially a pair of (TupleID,Value).
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
