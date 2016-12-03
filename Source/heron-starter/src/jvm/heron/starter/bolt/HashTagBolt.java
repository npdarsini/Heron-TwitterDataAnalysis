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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
        String HashTags = "~/Desktop/Entity/HashTags.txt";
        // String HashTags = "Entity/HashTags.txt";
        if (word.startsWith("#")) {
            outputCollector.emit(new Values(word));
            // System.out.println(word);
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("stdout"));
    }
}

