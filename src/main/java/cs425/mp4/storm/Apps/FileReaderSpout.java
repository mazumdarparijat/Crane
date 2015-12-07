/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cs425.mp4.storm.Apps;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;

@SuppressWarnings("serial")
public class FileReaderSpout extends BaseRichSpout {
  private class CustomComparator implements Comparator<Values>{
	  public int compare(Values v1, Values v2){
		  return ((String)v1.get(2)).compareTo((String)v2.get(2));
	  }
  }
  SpoutOutputCollector _collector;
  final int BUFFER_CAPACITY=1000;
  final long TIMEOUT=2000;
  String filepath;
  BufferedReader bufferedReader;
  PriorityQueue<Values> buffer;
  int linenumber=0;
  public FileReaderSpout(String filepath){
	  this.filepath=filepath;
  }
  public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    try {
		bufferedReader = new BufferedReader(new FileReader(filepath));
		buffer = new PriorityQueue<Values>(BUFFER_CAPACITY,new CustomComparator());
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }

  public void nextTuple() {
    Values head=buffer.peek();
    if(head==null || (System.currentTimeMillis()<Long.parseLong((String)head.get(2)))){	
    	if(buffer.size()<BUFFER_CAPACITY){
    		try {
				String line = bufferedReader.readLine();
				if(line!=null){
					linenumber++;
					Values tuple=new Values(Integer.toString(linenumber),line);
					Integer msgId=new Integer(linenumber);
					buffer.add(new Values(msgId,tuple,
							Long.toString(System.currentTimeMillis()+TIMEOUT)));
					_collector.emit(tuple,msgId);
				}
				else{
					Utils.sleep(100);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		
    	}
    }
    else{
    	Integer msgId = (Integer)head.get(0);
    	Values tuple = (Values)head.get(1);
    	buffer.remove();
    	buffer.add(new Values(msgId,tuple,
				Long.toString(System.currentTimeMillis()+TIMEOUT)));
    	_collector.emit(tuple,msgId);
    }
  }

  @Override
  public void ack(Object id) {
	  System.out.println("["+System.currentTimeMillis()+"]Ack received for "+id.toString());
	  Values dyingelement=null;
	  for (Values v:buffer){
		  Integer msgId=(Integer)v.get(0);
		  if(msgId.equals(id)){
			  dyingelement=v;
			  break;
		  }
	  }
	  if(buffer.remove(dyingelement)){
		  System.out.println("["+System.currentTimeMillis()+"]Tuple for "+id.toString()+" removed from queue");
	  };
  }

  @Override
  public void fail(Object id) {
	  System.out.println("["+System.currentTimeMillis()+"] Fail received for "+id.toString());
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("linenumber","line"));
  }
}