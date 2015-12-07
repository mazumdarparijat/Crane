package cs425.mp4.crane.Apps;

import java.util.HashMap;

import cs425.mp4.crane.Topology.Bolt;

/**
 * Filter bolt for reddit posts based on downvotes/upvotes
 */

@SuppressWarnings("serial")
public class FlagPostFilterRedditBolt implements Bolt {
	final int tvThreshold=100;
	final int ncThreshold=10;
	final double dvThreshold=0.1;
	public void open() {
    }
	public HashMap<String, String> execute(HashMap<String, String> in) {
		String linenumber = in.get("linenumber");
    	String reddit_id = in.get("id");
    	String totalvotes=in.get("totalvotes");
    	String num_comments=in.get("num_comments");
    	String downvotes=in.get("downvotes");
    	if(reddit_id.isEmpty() || totalvotes.isEmpty() ||
    			num_comments.isEmpty() || downvotes.isEmpty() || 
    			totalvotes.equals("0")){
    		return null;
    	}
    	else{
    		try{
	    		int tv=Integer.parseInt(totalvotes);
	    		int nc=Integer.parseInt(num_comments);
	    		int dv=Integer.parseInt(downvotes);
	    		if(tv>tvThreshold && nc>ncThreshold && 
	    				((dv/(double)tv)>dvThreshold)){
	    			HashMap<String,String> emit=new HashMap<String,String>();
	    			emit.put("linenumber", linenumber);
	    			emit.put("id", reddit_id);
	    			return emit;
	    		}
	    		else{
	    			return null;
	    		}
    		}catch(Exception e){
    			e.printStackTrace();
    			return null;
    		}
    	}
    }
  }
