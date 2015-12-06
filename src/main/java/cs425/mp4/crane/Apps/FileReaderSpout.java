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
package cs425.mp4.crane.Apps;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import cs425.mp4.crane.Topology.Spout;

@SuppressWarnings("serial")
public class FileReaderSpout implements Spout {
  String filepath;
  int linenumber=0;
  BufferedReader bufferedReader;
  public FileReaderSpout(String filepath){
	  this.filepath=filepath;
  }
  public void open(){
    try {
		bufferedReader = new BufferedReader(new FileReader(filepath));
	} catch (FileNotFoundException e) {
		e.printStackTrace();
	}
  }
  public HashMap<String, String> nextTuple() {
		try {
			String line = bufferedReader.readLine();
			if(line!=null){
				linenumber++;
				HashMap<String,String> emit=new HashMap<String, String>();
				emit.put("linenumber", Integer.toString(linenumber));
				emit.put("line", line);
				return emit;
			}
			else{
				Thread.sleep(100);
				return null;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} catch(InterruptedException e){
			e.printStackTrace();
			return null;
		}	
   }
}