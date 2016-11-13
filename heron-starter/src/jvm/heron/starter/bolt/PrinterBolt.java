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
import java.util.Map;


public class PrinterBolt extends BaseBasicBolt {

    OutputCollector outputCollector;

  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

    String IPhoneTags= "/home/npdarsini/Desktop/Entity/IphoneTags.txt";
    String AndroidTags = "/home/npdarsini/Desktop/Entity/AndroidTags.txt";
    String WebTags ="/home/npdarsini/Desktop/Entity/WebTags.txt";
    String TweetTags = "/home/npdarsini/Desktop/Entity/TweetDeck.txt";
    String Windows = "/home/npdarsini/Desktop/Entity/WindowsTags.txt";
    String twittbot = "/home/npdarsini/Desktop/Entity/twittbot.txt";
    String others = "/home/npdarsini/Desktop/Entity/others.txt";

   if(tuple.getString(0).contains("iPhone"))
    {

      PrintWriter out = null;
      try {
        out = new PrintWriter(new BufferedWriter(new FileWriter(IPhoneTags, true)));
        out.println(tuple.getString(0));
      }catch (IOException e) {
        System.err.println(e);
      } finally{
        if(out != null){
          out.close();
        }
      }
      System.out.println("Results:" + tuple.getString(0));
    }

    else if(tuple.getString(0).contains("Android"))
   {
     PrintWriter out = null;
     try {
       out = new PrintWriter(new BufferedWriter(new FileWriter(AndroidTags, true)));
       out.println(tuple.getString(0));
     }catch (IOException e) {
       System.err.println(e);
     } finally{
       if(out != null){
         out.close();
       }
     }
     System.out.println("Results:" + tuple.getString(0));

   }

   else if(tuple.getString(0).contains("Web"))
   {
     PrintWriter out = null;
     try {
       out = new PrintWriter(new BufferedWriter(new FileWriter(WebTags, true)));
       out.println(tuple.getString(0));
     }catch (IOException e) {
       System.err.println(e);
     } finally{
       if(out != null){
         out.close();
       }
     }
     System.out.println("Results:" + tuple.getString(0));
   }

   else if(tuple.getString(0).contains("TweetDeck"))
   {
     PrintWriter out = null;
     try {
       out = new PrintWriter(new BufferedWriter(new FileWriter(TweetTags, true)));
       out.println(tuple.getString(0));
     }catch (IOException e) {
       System.err.println(e);
     } finally{
       if(out != null){
         out.close();
       }
     }
     System.out.println("Results:" + tuple.getString(0));
   }

   else if(tuple.getString(0).contains("Windows"))
   {
     PrintWriter out = null;
     try {
       out = new PrintWriter(new BufferedWriter(new FileWriter(Windows, true)));
       out.println(tuple.getString(0));
     }catch (IOException e) {
       System.err.println(e);
     } finally{
       if(out != null){
         out.close();
       }
     }
     System.out.println("Results:" + tuple.getString(0));
   }


 else if(tuple.getString(0).contains("twittbot"))
    {
      PrintWriter out = null;
      try {
        out = new PrintWriter(new BufferedWriter(new FileWriter(twittbot, true)));
        out.println(tuple.getString(0));
      }catch (IOException e) {
        System.err.println(e);
      } finally{
        if(out != null){
          out.close();
        }
      }
      System.out.println("Results:" + tuple.getString(0));
    }

    else
   {
     PrintWriter out = null;
     try {
       out = new PrintWriter(new BufferedWriter(new FileWriter(others, true)));
       out.println(tuple.getString(0));
     }catch (IOException e) {
       System.err.println(e);
     } finally{
       if(out != null){
         out.close();
       }
     }
     System.out.println("Results:" + tuple.getString(0));
   }




    //System.out.println("[ "+basicOutputCollector.toString()+ " ]");

     }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
      this.outputCollector=outputCollector;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }


}
