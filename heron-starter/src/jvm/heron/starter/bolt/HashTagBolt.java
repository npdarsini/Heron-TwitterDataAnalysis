package heron.starter.bolt;

/**
 * Created by npdarsini on 11/12/16.
 */


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class HashTagBolt extends BaseRichBolt {
    OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext,
                        OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        //System.out.println("Priya Tuple:"+tuple.getString(0));
        String word = tuple.getString(0);
        if(word.startsWith("#")) {
            // only emit hashtags, and emit them without the # character
            outputCollector.emit(new Values(word.substring(1, word.length())));
            System.out.println(word);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("stdout"));
    }
}

