package cs425.mp4.crane;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

/**
 * Created by parijatmazumdar on 02/12/15.
 */
public class Forwarder implements Serializable {
    private ArrayList<String> childrenBolts;
    private ArrayList<Integer> correspondingNumTasks;
    private ArrayList<String> hashFields;
    private static final long serialVersionUID = 75L;

    public Forwarder() {
        childrenBolts=new ArrayList<String>();
        correspondingNumTasks=new ArrayList<Integer>();
        hashFields=new ArrayList<String>();
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
            if (hashFields.get(i)==null) {
                index=rn.nextInt(correspondingNumTasks.get(i));
            } else {
                index=val.get(hashFields.get(i)).hashCode()%correspondingNumTasks.get(i);
            }

            ret.add(childrenBolts.get(i)+String.valueOf(index));
        }

        return ret;
    }
}
