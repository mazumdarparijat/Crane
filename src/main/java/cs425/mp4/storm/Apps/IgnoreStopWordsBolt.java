package cs425.mp4.storm.Apps;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Stopword removal bolt
 */

@SuppressWarnings("serial")
public class IgnoreStopWordsBolt extends BaseBasicBolt {

    private Set<String> IGNORE_LIST = new HashSet<String>(Arrays.asList(new String[] {
            "http", "the", "you", "que", "and", "for", "that", "like", "have", "this", "just", "with", "all", "get", "about",
            "can", "was", "not", "your", "but", "are", "one", "what", "out", "when", "get", "lol", "now", "para", "por",
            "want", "will", "know", "good", "from", "las", "don", "people", "got", "why", "con", "time", "would","a",
    }));

    public void execute(Tuple input, BasicOutputCollector collector) { 
        String word = (String) input.getValueByField("word");
        String linenumber = (String) input.getValueByField("linenumber");
        String count = (String) input.getValueByField("count");
        if (!IGNORE_LIST.contains(word)) {
            collector.emit(new Values(linenumber,word,count));
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("linenumber","word","count"));
    }
}