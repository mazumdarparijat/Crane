package cs425.mp4.crane.Topology;

import cs425.mp4.crane.Exceptions.InvalidIDException;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by parijatmazumdar on 01/12/15.
 */
public class TopologyBuilder {
    public final Topology topology;
    private Set<String> idSet;
    public TopologyBuilder() {
        topology =new Topology();
        idSet=new HashSet<String>();
    }

    public void setSpout(String id, Spout s) throws InvalidIDException {
        if (idSet.contains(id))
            throw new InvalidIDException("ID already taken");

        idSet.add(id);
        topology.addRecord(new TopologyRecord(id, TopologyRecord.spoutType, s, 1, null, null));
    }

    public void setBoltShuffleGrouping(String id, Bolt b, int numTasks, String parentID) throws InvalidIDException {
        if (idSet.contains(id))
            throw new InvalidIDException("ID already taken");

        if (!idSet.contains(parentID))
            throw new InvalidIDException("parent ID does not exist");

        idSet.add(id);
        topology.addRecord(new TopologyRecord(id, TopologyRecord.boltType, b, numTasks, parentID, null));
    }

    public void setBoltFieldsGrouping(String id, Bolt b, int numTasks, String parentID, String hashField)
            throws InvalidIDException {
        if (idSet.contains(id))
            throw new InvalidIDException("ID already taken");

        if (!idSet.contains(parentID))
            throw new InvalidIDException("parent ID does not exist");

        idSet.add(id);
        topology.addRecord(new TopologyRecord(id, TopologyRecord.boltType, b, numTasks, parentID, hashField));
    }
}
