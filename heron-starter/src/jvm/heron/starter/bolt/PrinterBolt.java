package heron.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;


public class PrinterBolt extends BaseBasicBolt {

    OutputCollector outputCollector;
    Map<String, Integer> counts = new HashMap<String, Integer>();
    String Source = "Entity/Source.txt";
    String word = " ";

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
        } else {
            word = "Others";
            Integer count = counts.get(word);
            if (count == null) count = 0;
            count++;
            counts.put(word, count);
            othersC++;
        }
        //PrintWriter out = null;
//      try {
//          out = new PrintWriter(new BufferedWriter(new FileWriter(Source, true)));
//      if(tuple.getString(0).contains("iPhone"))
//      {
//          out.println("(iPhone, "+iPhoneC+ " )");
//      }
//
//      else if(tuple.getString(0).contains("Android"))
//      {
//          out.println("(Android, "+AndroidC+ " )");
//      }
//
//      else if(tuple.getString(0).contains("Web"))
//      {
//          out.println("(Web, "+webCount+ " )");
//      }
//
//      else if(tuple.getString(0).contains("TweetDeck"))
//      {
//          out.println("(TweetDeck, "+TweetDeckC+ " )");
//      }
//
//      else if(tuple.getString(0).contains("Windows"))
//      {
//          out.println("(Windows, "+WindowsC+ " )");
//      }
//      else if(tuple.getString(0).contains("twittbot"))
//      {
//          out.println("(twittbot, "+twittbotC+ " )");
//      }
//
//      else if(tuple.getString(0).contains("Facebook"))
//      {
//          out.println("(Facebook, "+FacebookC+ " )");
//      }
//
//      else if(tuple.getString(0).contains("iPad"))
//      {
//          out.println("(iPad, "+iPadC+ " )");
//      }
//      else if(tuple.getString(0).contains("Blackberry")) {
//          out.println("(Blackberry, "+BlackberryC+ " )");
//      }
//      else
//      {
//          out.println("(other Sources, "+othersC+ " )");
//      }
//
//      }catch (IOException e) {
//          System.err.println(e);
//      } finally{
//          if(out != null){
//              out.close();
//          }
//      }

       // String str = "(" + word + ", " + +")";
        if (counts != null) {
            PrintWriter out = null;
            try {
                out = new PrintWriter(new BufferedWriter(new FileWriter(Source, true)));
                out.println(counts);
            } catch (IOException e) {
                System.err.println(e);
            } finally {
                if (out != null) {
                    out.close();
                }
            }

            System.out.println("Results:" + tuple.getString(0));


            //System.out.println("[ "+basicOutputCollector.toString()+ " ]");


        }
    }



    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
      this.outputCollector=outputCollector;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {


    }
}


