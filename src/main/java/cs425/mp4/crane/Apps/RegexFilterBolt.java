package cs425.mp4.crane.Apps;

import java.util.HashMap;

import cs425.mp4.crane.Topology.Bolt;

@SuppressWarnings("serial")
public class RegexFilterBolt implements Bolt {
	private String _regex;
	public RegexFilterBolt(String regex){
		_regex=regex;
	}
	public void open() {

    }
	public HashMap<String, String> execute(HashMap<String, String> in) {
		String line=in.get("line");
    	if(line.matches(_regex)){
    		return in;
    	}
    	else{
    		return null;
    	}
    }
  }
