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

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
//import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class ErrorLogJoinTopology {

  public static void main(String[] args) throws Exception {
	if(args.length!=1){
		System.err.println("[ERROR]: Missing Argument");
		System.err.println("Usage: ErrorLogJoinTopology filename");
		System.exit(-1);
	}
	else{
	    TopologyBuilder builder = new TopologyBuilder();
	
	    builder.setSpout("spout", new FileReaderSpout(args[0]), 1);
	    builder.setBolt("filter", new RegexFilterBolt(".*\\.gif.*"), 1).shuffleGrouping("spout");
	    builder.setBolt("transform", new ExtractLogErrorBolt(), 1).shuffleGrouping("filter");
	    builder.setBolt("join", new JoinLogErrorBolt(), 1).shuffleGrouping("transform");
	    builder.setBolt("dashboard", new DashboardPrinterBolt("localhost",8888),1).shuffleGrouping("join");
	    Config conf = new Config();
	    conf.setDebug(true);
	    conf.setNumWorkers(3);
	    StormSubmitter.submitTopologyWithProgressBar("Error Log Join", conf, builder.createTopology());
	  }
  }
}
