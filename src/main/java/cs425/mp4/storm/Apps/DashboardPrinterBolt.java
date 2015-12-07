package cs425.mp4.storm.Apps;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class DashboardPrinterBolt extends BaseRichBolt {
	private final int _port;
	private final String _add; 
	OutputCollector _collector;
	public DashboardPrinterBolt(String add,int port){
		_add=add;
		_port=port;
	}
	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }
	public void execute(Tuple tuple) {
		try{
			String linenumber=(String)tuple.getValueByField("linenumber");
			if(Integer.parseInt(linenumber)>132000){
	    		Socket clientSocket=new Socket(_add,_port);
	    		DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
				outToServer.writeBytes(linenumber+","+tuple.toString()+'\n');
				clientSocket.close();
			}
			_collector.ack(tuple);
    	}catch(IOException e){
    		e.printStackTrace();
    		_collector.fail(tuple);
    	}
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
  }
