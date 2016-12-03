package heron.starter.spout;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSampleSpout extends BaseRichSpout 
{
    SpoutOutputCollector _collector;
    LinkedBlockingQueue<Status> queue = null;
    TwitterStream _twitterStream;
    String _custkey;
    String _custsecret;
    String _accesstoken;
    String _accesssecret;
    
    public TwitterSampleSpout(String key, String secret) {
        _custkey = key;
        _custsecret = secret;
    }

    public TwitterSampleSpout(String key, String secret, String token, String tokensecret) {
        _custkey = key;
        _custsecret = secret;
        _accesstoken = token;
        _accesssecret = tokensecret;
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<Status>(1000);
        _collector = collector;

        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            @Override
            public void onTrackLimitationNotice(int i) {
            }

            @Override
            public void onScrubGeo(long l, long l1) {
            }

            @Override
            public void onStallWarning(StallWarning warning) {
            }

            @Override
            public void onException(Exception e) {
              e.printStackTrace();
            }
        };

        ConfigurationBuilder config = 
            new ConfigurationBuilder()
                 .setOAuthConsumerKey(_custkey)
                 .setOAuthConsumerSecret(_custsecret)
                 .setOAuthAccessToken(_accesstoken)
                 .setOAuthAccessTokenSecret(_accesssecret);

        TwitterStreamFactory fact = 
            new TwitterStreamFactory(config.build());

        _twitterStream = fact.getInstance();
        _twitterStream.addListener(listener);
        _twitterStream.sample();
    }

    @Override
    public void nextTuple() {
        Status ret = queue.poll();
        if(ret==null) {
            Utils.sleep(50);
        } else {
//            System.out.println("Lang: " + ret.getLang().toString());
            if(ret.getGeoLocation() != null) {
                System.out.println("Loc: " + ret.getGeoLocation().getLatitude() + "," + ret.getGeoLocation().getLongitude());
                String latLong = ret.getGeoLocation().getLatitude() + "," + ret.getGeoLocation().getLongitude();
                _collector.emit("GeoBolt", new Values(latLong));
            }

            if(ret.getLang().toString() != "und") {
                _collector.emit("SplitBolt", new Values(ret.getText()));
            _collector.emit("SourceBolt", new Values(ret.getSource()));
//                Utils.sleep(200);
            }
//            if(ret.getPlace())
           // _collector.emit(new Values(ret.getSource()))
        }
    }

    @Override
    public void close() {
        _twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }    

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declareStream("SplitBolt", new Fields("tweet1"));
        declarer.declareStream("SourceBolt", new Fields("tweet2"));
        declarer.declareStream("GeoBolt", new Fields("tweet3"));
    }
}
