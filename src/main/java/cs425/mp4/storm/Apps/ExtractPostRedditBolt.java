package cs425.mp4.storm.Apps;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class ExtractPostRedditBolt extends BaseBasicBolt {
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	String post=(String)tuple.getValueByField("line");
    	String linenumber = (String)tuple.getValueByField("linenumber");
    	String[] info = post.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    	try{
	    	String reddit_id=info[5];
	    	String totalvotes=info[4];
	    	String downvotes=info[8];
	    	String num_comments=info[11];
	    	collector.emit(new Values(linenumber,reddit_id,totalvotes,num_comments,downvotes));
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("linenumber","id","totalvotes","num_comments","downvotes"));
    }
  }
