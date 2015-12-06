package cs425.mp4.crane.Apps;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import cs425.mp4.crane.Topology.Bolt;

@SuppressWarnings("serial")
public class JoinLogErrorBolt implements Bolt {
	HashMap<String,String> errMap;
	public void open() {
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
			e.printStackTrace();
		}
    }
    public HashMap<String, String> execute(HashMap<String, String> in) {
    	String host=in.get("host");
    	String error=in.get("error");
    	String linenumber =in.get("linenumber");
    	if(errMap.containsKey(error)){
    		HashMap<String,String> emit=new HashMap<String, String>();
	    	emit.put("linenumber", linenumber);
	    	emit.put("host", host);
	    	emit.put("errorDes", errMap.get(error));
    		return emit;
    	}
    	else{
    		return null;
    	}
    }
  }
