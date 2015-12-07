package cs425.mp4.crane.Topology;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Class to record a Topology description
 */
public class Topology implements Serializable {
    private static final long serialVersionUID = 76L;
    private ArrayList<TopologyRecord> records;
    public Topology() {
        records=new ArrayList<TopologyRecord>();
    }

    /**
     * Add element(spout/bolt) in topology
     * @param rec record
     */
    void addRecord(TopologyRecord rec) {
        records.add(rec);
    }

    /**
     * Get elements in topology
     * @return
     */
    public ArrayList<TopologyRecord> getRecords() {
        return (ArrayList<TopologyRecord>) records.clone();
    }
}
