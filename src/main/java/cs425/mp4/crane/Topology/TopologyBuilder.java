package cs425.mp4.crane.Topology;

import cs425.mp4.crane.Exceptions.InvalidIDException;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by parijatmazumdar on 01/12/15.
 */
public class TopologyBuilder {
    private final Topology t;
    private Set<String> idSet;
    public TopologyBuilder() {
        t=new Topology();
        idSet=new HashSet<String>();
    }

    public void setSpout(String id, Spout s,String filename) throws InvalidIDException {
        if (idSet.contains(id))
            throw new InvalidIDException("ID already taken");

        idSet.add(id);
        t.addRecord(new TopologyRecord(id,'s',s,1,filename));
    }

    public void setBolt(String id, Bolt b, int numTasks, String parentID) throws InvalidIDException {
        if (idSet.contains(id))
            throw new InvalidIDException("ID already taken");

        if (!idSet.contains(parentID))
            throw new InvalidIDException("parent ID does not exist");

        idSet.add(id);
        t.addRecord(new TopologyRecord(id, 'b', b, numTasks, parentID));
    }


}
