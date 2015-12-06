
package cs425.mp4.crane.Apps;

import cs425.mp4.crane.Constants;
import cs425.mp4.crane.CraneSubmitter;
import cs425.mp4.crane.Exceptions.InvalidIDException;
import cs425.mp4.crane.Topology.TopologyBuilder;

public class WordCountTopology {
  public static void main(String[] args){
	if(args.length!=2){
		System.err.println("[ERROR]: Missing Argument(s)");
		System.err.println("Usage: WordCountTopology nimbusHostname filename");
		System.exit(-1);
	}
	else{
	    TopologyBuilder builder = new TopologyBuilder();
	    try{
		    builder.setSpout("spout", new FileReaderSpout(args[1]));
		    builder.setBoltShuffleGrouping("lcount", new LocalCountBolt(), 2,"spout");
		    builder.setBoltShuffleGrouping("filter", new IgnoreStopWordsBolt(), 2,"lcount");
		    CraneSubmitter cs=new CraneSubmitter(args[0], Constants.NIMBUS_PORT);
		    cs.submitTopology(builder.topology);
            Thread.sleep(1000);
	    }catch(InvalidIDException e){
			e.printStackTrace();
			System.exit(-1);
		}catch(InterruptedException e){
			e.printStackTrace();
			System.exit(-1);
		}
	}
  }
}
