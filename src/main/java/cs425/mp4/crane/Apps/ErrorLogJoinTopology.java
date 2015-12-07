package cs425.mp4.crane.Apps;

import cs425.mp4.crane.Constants;
import cs425.mp4.crane.CraneSubmitter;
import cs425.mp4.crane.Exceptions.InvalidIDException;
import cs425.mp4.crane.Topology.TopologyBuilder;

/**
 * Topology implementing filter, transform, join in log data
 */
public class ErrorLogJoinTopology {

  public static void main(String[] args) {
	if(args.length!=2){
		System.err.println("[ERROR]: Missing Argument(s)");
		System.err.println("Usage: ErrorLogJoinTopology nimbusHostname filename");
		System.exit(-1);
	}
	else{
	    TopologyBuilder builder = new TopologyBuilder();
	    try{
		    builder.setSpout("spout", new FileReaderSpout(args[1]));
		    builder.setBoltShuffleGrouping("filter", new RegexFilterBolt(".*\\.gif.*"), 2,"spout");
		    builder.setBoltShuffleGrouping("transform", new ExtractLogErrorBolt(), 2,"filter");
		    builder.setBoltShuffleGrouping("join", new JoinLogErrorBolt(), 2,"transform");
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
