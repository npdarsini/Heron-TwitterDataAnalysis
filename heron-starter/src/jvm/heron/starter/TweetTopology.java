package heron.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import heron.starter.bolt.PrinterBolt;
import heron.starter.spout.EntitySpout;

import java.util.Map;

/**
 * This is a basic example of a Storm topology.
 */
public class TweetTopology {

    private static final int TOP_N = 5;

  public static class StdoutBolt extends BaseRichBolt {
    OutputCollector _collector;
    String taskName;

   // @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      taskName = context.getThisComponentId() + "_" + context.getThisTaskId();
    }

   // @Override
    public void execute(Tuple tuple) {
      System.out.println(tuple.getValue(0));
      _collector.emit(tuple, new Values(tuple.getValue(0)));
      _collector.ack(tuple);
    }

   // @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
      //String SpoutId = "stdout";
      String HashTag = "hashTag";
      String HashTagCounter = "hashTagCount";
      String counterId = "counter";
      String intermediateRankerId = "intermediateRanker";
      String totalRankerId = "finalRanker";


//    builder.setSpout("word",
//           new TwitterSampleSpout("nk7lCf9Z2qJlVhkN8QGBOkdTF",
//                 "1nuJjD1hGIfAfnLZ5C2PNO4Sisg9OpGOcDG0ZMehpJIbHjzIps",
//                 "340731816-VqiIbfJHsJzB698A3FpGnH2Eqv2vqS0NJGOGGenI",
//                 "RHhDJiynBr45EUUrAjOc5j70eFGFjsaMXr7GlvNVQsyEF"), 1);
//
//    builder.setBolt("stdout", new SentenceSplitBolt(), 1).shuffleGrouping("word");
      builder.setSpout("Entity", new EntitySpout("nk7lCf9Z2qJlVhkN8QGBOkdTF",
              "1nuJjD1hGIfAfnLZ5C2PNO4Sisg9OpGOcDG0ZMehpJIbHjzIps",
              "340731816-VqiIbfJHsJzB698A3FpGnH2Eqv2vqS0NJGOGGenI",
              "RHhDJiynBr45EUUrAjOc5j70eFGFjsaMXr7GlvNVQsyEF"), 1);
      builder.setBolt("EntityPrint", new PrinterBolt(),2).shuffleGrouping("Entity");


//      builder.setBolt(HashTag, new HashTagBolt(),1).shuffleGrouping("stdout");
//      //builder.setBolt(HashTagCounter, new HashTagCount(), 3).shuffleGrouping(HashTag);
//
//      builder.setBolt(HashTagCounter,new HashTagCount(),8).fieldsGrouping(HashTag, new Fields("hashtag"));
//
//      //builder.setBolt("Printer", new PrinterBolt(), 2).shuffleGrouping(HashTagCounter);

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      //Utils.sleep(1000000000);
        Utils.sleep(25000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
