package heron.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.*;


public class PrinterBolt extends BaseBasicBolt {

    OutputCollector outputCollector;
    static Map<String, Integer> counts = new HashMap<String, Integer>();
    File source = new File("/home/npdarsini/HeronTweetsAnalysis/heron-starter/lib/HighCharts/examples/dynamic-update/Source.txt");
   // File source = new File("~/Desktop/Entity/Source.txt");
    String word = " ";

    private int i=0;
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {


//    String IPhoneTags= "/home/npdarsini/Desktop/Entity/IphoneTags.txt";
//    String AndroidTags = "/home/npdarsini/Desktop/Entity/AndroidTags.txt";
//    String WebTags ="/home/npdarsini/Desktop/Entity/WebTags.txt";
//    String TweetTags = "/home/npdarsini/Desktop/Entity/TweetDeck.txt";
//    String Windows = "/home/npdarsini/Desktop/Entity/WindowsTags.txt";
//    String twittbot = "/home/npdarsini/Desktop/Entity/twittbot.txt";
//    String FacebookTags = "/home/npdarsini/Desktop/Entity/Facebook.txt";
//    String iPad = "/home/npdarsini/Desktop/Entity/iPad.txt";
//    String Blackberry = "/home/npdarsini/Desktop/Entity/Blackberry.txt";
//    String others = "/home/npdarsini/Desktop/Entity/others.txt";
//
        int iPhoneC = 0;
        int AndroidC = 0;
        int webCount = 0;
        int TweetDeckC = 0;
        int WindowsC = 0;
        int twittbotC = 0;
        int FacebookC = 0;
        int iPadC = 0;
        int BlackberryC = 0;
        int othersC = 0;

//      String IPhoneTags= "Entity/IphoneTags.txt";
//      String AndroidTags = "Entity/AndroidTags.txt";
//      String WebTags ="Entity/WebTags.txt";
//      String TweetTags = "Entity/TweetDeck.txt";
//      String Windows = "Entity/WindowsTags.txt";
//      String twittbot = "Entity/twittbot.txt";
//      String others = "Entity/others.txt";
//      String FacebookTags = "Entity/Facebook.txt";
//      String iPad = "Entity/iPad.txt";
//      String Blackberry = "Entity/Blackberry.txt";
        //String Source = "Entity/Source.txt";

        if (tuple.getString(0).contains("iPhone")) {
            // iPhoneC++;

            word = "iPhone";
            Integer count = counts.get(word);
            if (count == null) count = 0;
            count++;
            counts.put(word, count);
        } else if (tuple.getString(0).contains("Android")) {
            //AndroidC++;
            word = "Android";
            Integer count = counts.get(word);
            if (count == null) count = 0;
            count++;
            counts.put(word, count);
        } else if (tuple.getString(0).contains("Web")) {
            word = "Web";
            Integer count = counts.get(word);
            if (count == null) count = 0;
            count++;
            counts.put(word, count);
            webCount++;
        } else if (tuple.getString(0).contains("TweetDeck")) {

            word = "TweetDeck";
            Integer count = counts.get(word);
            if (count == null) count = 0;
            count++;
            counts.put(word, count);
            TweetDeckC++;
        } else if (tuple.getString(0).contains("Windows")) {
            word = "Windows";
            Integer count = counts.get(word);
            if (count == null) count = 0;
            count++;
            counts.put(word, count);
            WindowsC++;
        } else if (tuple.getString(0).contains("twittbot")) {
            word = "twittbot";
            Integer count = counts.get(word);
            if (count == null) count = 0;
            count++;
            counts.put(word, count);
            twittbotC++;
        } else if (tuple.getString(0).contains("Facebook")) {
            word = "Facebook";
            Integer count = counts.get(word);
            if (count == null) count = 0;
            count++;
            counts.put(word, count);
            FacebookC++;
        } else if (tuple.getString(0).contains("iPad")) {
            word = "iPad";
            Integer count = counts.get(word);
            if (count == null) count = 0;
            count++;
            counts.put(word, count);
            iPadC++;
        } else if (tuple.getString(0).contains("Blackberry")) {
            word = "Blackberry";
            Integer count = counts.get(word);
            if (count == null) count = 0;
            count++;
            counts.put(word, count);
            BlackberryC++;
        }
//        else {
//            word = "Others";
//            Integer count = counts.get(word);
//            if (count == null) count = 0;
//            count++;
//            counts.put(word, count);
//            othersC++;
//        }

        i++;

        if (i > 200) {
            i = 0;

            TreeMap Tags60 = publishTopList(counts);
            int x=10;
            if(Tags60.size() < 10){
                x=Tags60.size();
            }
            if(Tags60.size() < 10){
                x=Tags60.size();
            }
            try {
                PrintWriter writer = new PrintWriter(source);
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
                System.out.println("Sources" + x + ": " + pair.getKey() + " = " + pair.getValue());
                try {
                    FileUtils.writeStringToFile(source, pair.getKey().toString().replace("#","") + "," + pair.getValue() + "\n", "UTF-8", true);

                }
                catch (IOException e) {
                    e.printStackTrace();
                }
//
                it.remove(); // avoids a ConcurrentModificationException
            }

        }
//        if (counts != null) {
//            PrintWriter out = null;
//            try {
//                out = new PrintWriter(new BufferedWriter(new FileWriter(source, true)));
//                out.println(counts);
//            } catch (IOException e) {
//                System.err.println(e);
//            } finally {
//                if (out != null) {
//                    out.close();
//                }
//            }
//
//            System.out.println("Results:" + tuple.getString(0));


            //System.out.println("[ "+basicOutputCollector.toString()+ " ]");


//        }
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
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
      this.outputCollector=outputCollector;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {


    }
}


