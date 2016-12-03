package heron.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

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
//    File hashTags = new File("/home/harsha/Desktop/Entity/HashTags.txt");
    File hashTags = new File("/home/npdarsini/HeronTweetsAnalysis/heron-starter/lib/HighCharts/examples/dynamic-update/HashTags.txt");
    //File hashTags = new File("~/Desktop/Entity/HashTags.txt");
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
        SortedMap top = new TreeMap<Long, String>(Collections.reverseOrder());
        Integer count = counts.get(word);
        if (count == null) count = 0;
        count++;
        counts.put(word, count);
        i++;

        if (i > 200) {
//            logger.info("\n\n");
//            logger.info("Word count: "+counter.size());
            i = 0;

            SortedMap Tags60 = publishTopList(counts);
            int x=10;
            if(Tags60.size() < 10){
                x=Tags60.size();
            }
            if(Tags60.size() < 10){
                x=Tags60.size();
            }
            try {
                PrintWriter writer = new PrintWriter(hashTags);
                writer.print("");
                writer.close();
            }
            catch(IOException e) {
                e.printStackTrace();
            }

            Iterator it = Tags60.entrySet().iterator();
//            int topSize = 10;
            while (it.hasNext()) {
                if(x==0) break;
                x--;
                Map.Entry pair = (Map.Entry)it.next();
                System.out.println("Top" + x + ": " + pair.getKey() + " = " + pair.getValue());
                try {
                    FileUtils.writeStringToFile(hashTags, pair.getKey().toString().replace("#","") + "," + pair.getValue() + "\n", "UTF-8", true);

                }
                catch (IOException e) {
                    e.printStackTrace();
                }
//
                it.remove(); // avoids a ConcurrentModificationException
            }

        }
//        String str = "(" + word + ", " + count + ")";
//        if (str != null) {
//            PrintWriter out = null;
//            try {
//                out = new PrintWriter(new BufferedWriter(new FileWriter(hashTags, true)));
//                out.println(str);
//            } catch (IOException e) {
//                System.err.println(e);
//            } finally {
//                if (out != null) {
//                    out.close();
//                }
//            }
//
//
//            System.out.println(str);
//
//            outputCollector.emit(new Values(word, count));
//
//        }
        outputCollector.ack(tuple);
    }

    TreeMap publishTopList(Map<String, Integer> counts)
    {
        // calculate top list:
        ValueComparator bvc = new ValueComparator(counts);
        TreeMap<String, Integer> top = new TreeMap<String, Integer>(bvc);
        top.putAll(counts);
        //        // Output top list:
//        for (Map.Entry<Long, String> entry : top.entrySet()) {
//            System.out.println("Priya here Success");
//            System.out.println("Top 10 Words are: "+new StringBuilder("top - ").append(entry.getValue()).append('|').append(entry.getKey()).toString());
//        }
        return top;

    }

    @Override

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag", "count"));
    }


}

