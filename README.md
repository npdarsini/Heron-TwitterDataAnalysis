# HeronTweetsAnalysis

Tweets analysis using Heron

Real Time Trends - Twitter is a continuous stream of tweets and in this stream, you need to identify the emerging top 10 trends. In order to simplify the problem break the tweet into words and identify the hash tags. Continously keep track of the top 10 hash tags and if there is tweet that bears this hash tag, add them to result and emit - so that you get the trends and the tweets supporting the trends.

Real Time BI - Tweets contain a lot of metadata - in fact each Tweet from the fire hose might be as long as 1K. Based on the metadata, you can find out several trends - break down by mobile/web, what mobile operating system, a crude sense of location, etc

Heatmap of Tweets - Some % of tweets contains latitude and longitude. Get those tweets continuously and update the google maps to see where the tweets are coming from and view them globally in a map, If there are dense set of tweets in an area it will be red, while cooler areas will be blue and some gradation in coloring between the two. 

 
We have created a Heron Topology with one spout and five bolts.

The Spout named "Word" will collect the tweets from the Twitter using twitter4japi. This spout will pass the data to the bolts named "stdout" "EntityPrint" "GeoExtBolt".

stdout bolt will split the tweets into words and emits this data to hashtag bolt. This hashtag bolt will filter the hashtags out of these bolts and emits these hashtags to the "hashTagCount" Bolt. This bolt will perform the count on the hashtags.

EntityPrint Bolt will extract the source/device information of the received tweets.

GeoExtBolt will extract geographic coordinates ie., latitude and longitude information of the received tweets.

These results are visualized using HighChart and Javascript.

