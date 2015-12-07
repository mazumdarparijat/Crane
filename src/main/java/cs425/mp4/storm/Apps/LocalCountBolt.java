package cs425.mp4.storm.Apps;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Counter bolt
 */

@SuppressWarnings("serial")
public class LocalCountBolt extends BaseBasicBolt {
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	String sentence=(String)tuple.getValueByField("line");
    	String linenumber = (String)tuple.getValueByField("linenumber");
    	String[] words=sentence.split(" ");
    	Map<String,Integer> map = new HashMap<String,Integer>();
    	for (String word:words){
    		Integer value=map.get(word);
    		Integer fvalue=new Integer(1);
    		if(value!=null){
    			fvalue=new Integer(value.intValue()+1);
    		}
    		map.put(word, fvalue);
    	}
    	for (String unique_word: map.keySet()){
    		collector.emit(new Values(linenumber,unique_word,map.get(unique_word).toString()));
    	}
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("linenumber","word","count"));
    }
  }
