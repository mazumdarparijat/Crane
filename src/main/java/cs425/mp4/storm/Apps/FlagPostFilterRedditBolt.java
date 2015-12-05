package cs425.mp4.storm.Apps;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class FlagPostFilterRedditBolt extends BaseBasicBolt {
	final int tvThreshold=100;
	final int ncThreshold=10;
	final double dvThreshold=0.1;
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	String linenumber = (String)tuple.getValueByField("linenumber");
    	String reddit_id = (String)tuple.getValueByField("id");
    	String totalvotes=(String)tuple.getValueByField("totalvotes");
    	String num_comments=(String)tuple.getValueByField("num_comments");
    	String downvotes=(String)tuple.getValueByField("downvotes");
    	if(reddit_id.isEmpty() || totalvotes.isEmpty() ||
    			num_comments.isEmpty() || downvotes.isEmpty() || 
    			totalvotes.equals("0")){}
    	else{
    		try{
	    		int tv=Integer.parseInt(totalvotes);
	    		int nc=Integer.parseInt(num_comments);
	    		int dv=Integer.parseInt(downvotes);
	    		if(tv>tvThreshold && nc>ncThreshold && 
	    				((dv/(double)tv)>dvThreshold)){
	    			collector.emit(new Values(linenumber,reddit_id));
	    		}
    		}catch(Exception e){
    			e.printStackTrace();
    		}
    	}
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("linenumber","id"));
    }
  }
