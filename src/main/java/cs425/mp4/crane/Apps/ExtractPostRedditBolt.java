package cs425.mp4.crane.Apps;

import java.util.HashMap;

import cs425.mp4.crane.Topology.Bolt;

/**
 * Bolt logic for extracting post params from reddit log
 */

@SuppressWarnings("serial")
public class ExtractPostRedditBolt implements Bolt {
	public void open() {

    }
	public HashMap<String, String> execute(HashMap<String, String> in) {
		String post=in.get("line");
		String linenumber=in.get("linenumber");
		String[] info = post.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
		try{
	    	String reddit_id=info[5];
	    	String totalvotes=info[4];
	    	String downvotes=info[8];
	    	String num_comments=info[11];
	    	HashMap<String,String> emit=new HashMap<String, String>();
	    	emit.put("linenumber", linenumber);
	    	emit.put("id", reddit_id);
	    	emit.put("totalvotes", totalvotes);
	    	emit.put("num_comments", num_comments);
	    	emit.put("downvotes", downvotes);
	    	return emit;
    	}catch(Exception e){
    		e.printStackTrace();
    		return null;
    	}
    }
  }
