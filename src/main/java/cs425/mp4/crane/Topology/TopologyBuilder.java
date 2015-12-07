package cs425.mp4.crane.Topology;

import cs425.mp4.crane.Exceptions.InvalidIDException;

import java.util.HashSet;
import java.util.Set;

/**
 * Builder pattern for Topology class
 */
public class TopologyBuilder {
    public final Topology topology;
    private Set<String> idSet;
    public TopologyBuilder() {
        topology =new Topology();
        idSet=new HashSet<String>();
    }

    /**
     * add spout to topology
     * @param id
     * @param s
     * @throws InvalidIDException
     */
    public void setSpout(String id, Spout s) throws InvalidIDException {
        if (idSet.contains(id))
            throw new InvalidIDException("ID already taken");

        idSet.add(id);
        topology.addRecord(new TopologyRecord(id, TopologyRecord.spoutType, s, 1, null, null));
    }

    /**
     * add bolt with shuffle grouping to parent
     * @param id
     * @param b
     * @param numTasks
     * @param parentID
     * @throws InvalidIDException
     */
    public void setBoltShuffleGrouping(String id, Bolt b, int numTasks, String parentID) throws InvalidIDException {
        if (idSet.contains(id))
            throw new InvalidIDException("ID already taken");

        if (!idSet.contains(parentID))
            throw new InvalidIDException("parent ID does not exist");

        idSet.add(id);
        topology.addRecord(new TopologyRecord(id, TopologyRecord.boltType, b, numTasks, parentID, null));
    }

    /**
     * add bolt with fields grouping (ie hash-based) to parent
     * @param id
     * @param b
     * @param numTasks
     * @param parentID
     * @param hashField
     * @throws InvalidIDException
     */
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
