package heron.starter.bolt;

/**
 * Created by harsha on 11/13/16.
 */


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.Map;

public class GeoExtBolt extends BaseRichBolt {
    OutputCollector outputCollector;

    private int i =0;

    private String latLong = "";
    File geo = new File("/home/npdarsini/HeronTweetsAnalysis/heron-starter/src/html/Geo.txt");

    @Override
    public void prepare(Map map, TopologyContext topologyContext,
                        OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        //System.out.println("Priya Tuple:"+tuple.getString(0));
        String word = tuple.getString(0);
        i++;
        latLong = latLong + "new google.maps.LatLng(" + word + ");";

        if(latLong.length() > 200) {
            i = 0;
            latLong = latLong.substring(0, latLong.length() - 1) + ";";
            System.out.println("Total : " + latLong);
            try {
                FileUtils.writeStringToFile(geo, latLong, "UTF-8", true);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            latLong = "";
        }

//        System.out.println("new google.maps.LatLng(" + word + "),");
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

