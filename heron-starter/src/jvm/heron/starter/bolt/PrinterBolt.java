package heron.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;


public class PrinterBolt extends BaseBasicBolt {

    OutputCollector outputCollector;

  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    System.out.println("Results:" + tuple.getString(0));
    System.out.println("[ "+basicOutputCollector.toString()+ " ]");

     }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
      this.outputCollector=outputCollector;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }


}
