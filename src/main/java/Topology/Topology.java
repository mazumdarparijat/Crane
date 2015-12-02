package Topology;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by parijatmazumdar on 01/12/15.
 */
public class Topology implements Serializable {
    private static final long serialVersionUID = 76L;
    private ArrayList<TopologyRecord> records;
    public Topology() {
        records=new ArrayList<TopologyRecord>();
    }

    public void addRecord(TopologyRecord rec) {
        records.add(rec);
    }

    public ArrayList<TopologyRecord> getRecords() {
        return (ArrayList<TopologyRecord>) records.clone();
    }
}
