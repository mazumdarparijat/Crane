package cs425.mp4.storm.Apps;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Bolt for extracting error params in log record
 */
@SuppressWarnings("serial")
public class ExtractLogErrorBolt extends BaseBasicBolt {
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	String logline=(String)tuple.getValueByField("line");
    	String linenumber = (String)tuple.getValueByField("linenumber");
    	try{
	    	String[] info=logline.split(" ");
	    	String hostname=info[0];
	    	String error=info[info.length-2];
	    	collector.emit(new Values(linenumber,hostname,error));
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    }	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("linenumber","host","error"));
    }
  }
