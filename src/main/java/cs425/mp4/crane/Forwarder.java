package cs425.mp4.crane;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

/**
 * Created by parijatmazumdar on 02/12/15.
 */
public class Forwarder implements Serializable {
    public final String spoutID;
    public final int numAcks;
    private ArrayList<String> childrenBolts;
    private ArrayList<Integer> correspondingNumTasks;
    private ArrayList<String> hashFields;
    private static final long serialVersionUID = 75L;

    public Forwarder(String spoutID, int numAcks) {
        childrenBolts=new ArrayList<String>();
        correspondingNumTasks=new ArrayList<Integer>();
        hashFields=new ArrayList<String>();
        this.spoutID=spoutID;
        this.numAcks=numAcks;
    }

    public boolean isLeaf() {
        return childrenBolts.size()==0;
    }

    public void addChild(String boltID, int numTasks, String hashField) {
        childrenBolts.add(boltID);
        correspondingNumTasks.add(numTasks);
        hashFields.add(hashField);
    }

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
