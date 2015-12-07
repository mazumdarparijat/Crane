package cs425.mp4.storm.Apps;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Join with resource file bolt
 */

@SuppressWarnings("serial")
public class JoinLogErrorBolt extends BaseBasicBolt {
	HashMap<String,String> errMap;
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf,TopologyContext context){
		InputStream in = getClass().getResourceAsStream("/errorcodes.txt"); 
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		errMap=new HashMap<String,String>();
		String line;
		try {
			while ((line = reader.readLine()) != null) {
			    String[] entry=line.split("\t",2);
			    errMap.put(entry[0],entry[1]);
			}
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	String host=(String)tuple.getValueByField("host");
    	String error=(String)tuple.getValueByField("error");
    	String linenumber = (String)tuple.getValueByField("linenumber");
    	if(errMap.containsKey(error)){
    		collector.emit(new Values(linenumber,host,errMap.get(error)));
    	}
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("linenumber","host","errorDes"));
    }
  }
