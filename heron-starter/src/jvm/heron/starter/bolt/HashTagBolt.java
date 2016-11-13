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
        String HashTags = "/home/npdarsini/Desktop/Entity/HashTags.txt";
       // String HashTags = "Entity/HashTags.txt";
        if(word.startsWith("#")) {
            PrintWriter out = null;
            try {
                out = new PrintWriter(new BufferedWriter(new FileWriter(HashTags, true)));
                out.println(word.substring(1));
            }catch (IOException e) {
                System.err.println(e);
            } finally{
                if(out != null){
                    out.close();
                }
            }
            System.out.println("Results:" + tuple.getString(0));
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

