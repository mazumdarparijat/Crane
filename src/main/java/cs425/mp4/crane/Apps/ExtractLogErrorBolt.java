package cs425.mp4.crane.Apps;

import java.util.HashMap;
import cs425.mp4.crane.Topology.Bolt;

/**
 * Bolt to extract error in log.
 */

@SuppressWarnings("serial")
public class ExtractLogErrorBolt implements Bolt {
	public void open() {

    }
    public HashMap<String, String> execute(HashMap<String, String> in) {
    	String logline=in.get("line");
    	String linenumber = in.get("linenumber");
    	try{
	    	String[] info=logline.split(" ");
	    	String hostname=info[0];
	    	String error=info[info.length-2];
	    	HashMap<String,String> emit=new HashMap<String, String>();
	    	emit.put("linenumber", linenumber);
	    	emit.put("host", hostname);
	    	emit.put("error", error);
	    	return emit;
    	}catch(Exception e){
    		e.printStackTrace();
    		return null;
    	}
    }	
  }
