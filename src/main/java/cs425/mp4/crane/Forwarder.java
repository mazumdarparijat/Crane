package cs425.mp4.crane;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

/**
 * Class for deciding the taskID to which the result
 * of a spout/bolt task should be forwarded to based
 * on fieldsGrouping or shuffleGrouping
 */
public class Forwarder implements Serializable {
    public final String spoutID;
    public final int numAcks;
    private ArrayList<String> childrenBolts;
    private ArrayList<Integer> correspondingNumTasks;
    private ArrayList<String> hashFields;
    private static final long serialVersionUID = 75L;

    /**
     *
     * @param spoutID spout
     * @param numAcks the number of acks (or weight of ack) to be counted
     *                if this node directly sends ack instead of forwarding
     *                to subsequesnt bolts in topology
     */
    public Forwarder(String spoutID, int numAcks) {
        childrenBolts=new ArrayList<String>();
        correspondingNumTasks=new ArrayList<Integer>();
        hashFields=new ArrayList<String>();
        this.spoutID=spoutID;
        this.numAcks=numAcks;
    }

    /**
     *
     * @return whether this task is a part of bolt which is at the
     * end of topology tree
     */
    public boolean isLeaf() {
        return childrenBolts.size()==0;
    }

    /**
     * Add forwarding bolt
     * @param boltID
     * @param numTasks
     * @param hashField field for fieldsGrouping, null for shuffleGrouping
     */
    public void addChild(String boltID, int numTasks, String hashField) {
        childrenBolts.add(boltID);
        correspondingNumTasks.add(numTasks);
        hashFields.add(hashField);
    }

    /**
     *
     * @param val The value for which fieldsGrouping/shuffleGrouping has to
     *            be calculated
     * @return Array of taskIDs to which the output has to be forwarded to
     */
    ArrayList<String> getForwardTaskIDs(Map<String,String> val) {
        Random rn=new Random();
        ArrayList<String> ret=new ArrayList<String>();
        for (int i=0;i<childrenBolts.size();i++) {
            int index=-1;
            int numTasks=correspondingNumTasks.get(i);
            if (hashFields.get(i)==null) {
                index=rn.nextInt(numTasks);
            } else {
                index=(val.get(hashFields.get(i)).hashCode()%numTasks+numTasks)%numTasks;
            }

            ret.add(childrenBolts.get(i)+String.valueOf(index));
        }

        return ret;
    }
}
