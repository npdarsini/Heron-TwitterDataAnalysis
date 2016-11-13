package heron.starter.bolt;

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
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by npdarsini on 11/12/16.
 */
public class HashTagCount extends BaseRichBolt
{
    Map<String, Integer> counts = new HashMap<String, Integer>();
    private final int topListSize = 10;

    private Map<String, Long> counter;
    private long lastClearTime;
    private long lastLogTime;
    private long clearIntervalSec = 3;
    private long logIntervalSec = 3;

    private int i = 0;
    OutputCollector outputCollector;
    // private static final Logger logger = LoggerFactory.getLogger(HashTagCount.class);
    //String HashTags = "/home/npdarsini/Desktop/Entity/HashTags.txt";
    String HashTags = "Entity/HashTags.txt";
    @Override
    public void prepare(Map map, TopologyContext topologyContext,
                        OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        lastLogTime = System.currentTimeMillis();
        lastClearTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        SortedMap top = new TreeMap<Long, String>();
        Integer count = counts.get(word);
        if (count == null) count = 0;
        count++;
        counts.put(word, count);

        String str = "(" + word + ", " + count + ")";
        if (str != null) {
            PrintWriter out = null;
            try {
                out = new PrintWriter(new BufferedWriter(new FileWriter(HashTags, true)));
                out.println(str);
            } catch (IOException e) {
                System.err.println(e);
            } finally {
                if (out != null) {
                    out.close();
                }
            }


            System.out.println(str);

            outputCollector.emit(new Values(word, count));

        }
    }

    @Override

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag", "count"));
    }


}
