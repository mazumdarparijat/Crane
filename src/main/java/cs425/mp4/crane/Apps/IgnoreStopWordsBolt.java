package cs425.mp4.crane.Apps;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import cs425.mp4.crane.Topology.Bolt;

@SuppressWarnings("serial")
public class IgnoreStopWordsBolt implements Bolt {
    private Set<String> IGNORE_LIST = new HashSet<String>(Arrays.asList(new String[] {
            "http", "the", "you", "que", "and", "for", "that", "like", "have", "this", "just", "with", "all", "get", "about",
            "can", "was", "not", "your", "but", "are", "one", "what", "out", "when", "get", "lol", "now", "para", "por",
            "want", "will", "know", "good", "from", "las", "don", "people", "got", "why", "con", "time", "would","a",
    }));
    public void open() {

    }
    public HashMap<String, String> execute(HashMap<String, String> in) { 
        String lcounts = in.get("lcounts");
        String linenumber = in.get("linenumber");
        String[] wcounts=lcounts.split(" ");
        String flcounts="";
        for (String wcount:wcounts){
        	String[] entry = wcount.split(":",2);
        	if(!IGNORE_LIST.contains(entry[0])){
        		flcounts+=entry[0]+":"+entry[1]+" "; 
        	}
        }
        HashMap<String,String> emit=new HashMap<String,String>();
        emit.put("linenumber", linenumber);
        emit.put("lcounts", flcounts);
        return emit;
    }
}