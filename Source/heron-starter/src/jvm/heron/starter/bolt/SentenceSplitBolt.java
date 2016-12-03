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

public class SentenceSplitBolt extends BaseRichBolt {
    OutputCollector outputCollector;

   @Override
    public void prepare(Map conf, TopologyContext topologyContext,
                        OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }
   @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);
        for (String word : sentence.split(" ")) {
            outputCollector.emit(tuple, new Values(word));
            outputCollector.ack(tuple);
//            System.out.println(word);

        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
