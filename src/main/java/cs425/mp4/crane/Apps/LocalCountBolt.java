package cs425.mp4.crane.Apps;

import java.util.HashMap;
import cs425.mp4.crane.Topology.Bolt;

@SuppressWarnings("serial")
public class LocalCountBolt implements Bolt {
	public void open() {

    }
    public HashMap<String, String> execute(HashMap<String, String> in) {
    	String sentence=in.get("line");
    	String linenumber = in.get("linenumber");
    	String[] words=sentence.split(" ");
    	HashMap<String,Integer> map = new HashMap<String,Integer>();
    	for (String word:words){
    		Integer value=map.get(word);
    		Integer fvalue=new Integer(1);
    		if(value!=null){
    			fvalue=new Integer(value.intValue()+1);
    		}
    		map.put(word, fvalue);
    	}
    	HashMap<String,String> emit=new HashMap<String,String>();
    	emit.put("linenumber", linenumber);
    	String lcounts="";
    	for (String unique_word: map.keySet()){
    		lcounts+=unique_word+":"+map.get(unique_word).toString()+" ";
    	}
    	emit.put("lcounts", lcounts);
    	return emit;
    }
  }
