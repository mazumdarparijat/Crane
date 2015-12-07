package cs425.mp4.storm.Apps;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * regex based filtering bolt
 */

@SuppressWarnings("serial")
public class RegexFilterBolt extends BaseBasicBolt {
	private String _regex;
	public RegexFilterBolt(String regex){
		_regex=regex;
	}
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	String logline=(String)tuple.getValueByField("line");
    	String linenumber = (String)tuple.getValueByField("linenumber");
    	if(logline.matches(_regex)){
    		collector.emit(new Values(linenumber,logline));
    	}
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("linenumber","line"));
    }
  }
