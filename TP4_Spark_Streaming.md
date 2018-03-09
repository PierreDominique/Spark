# TP3 SPARK Streaming - Twitter Hashtag Count

Using Twitter Streaming is a great way to learn Spark Streaming if you don't have your streaming datasource and want a great rich input dataset to try Spark Streaming transformations on.

In this example, we show how to calculate the top hashtags seen in the last X window of time every Y time unit.


```scala
import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils

import scala.math.Ordering

import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
```

## Step 1: Enter your Twitter API Credentials.
* Go to https://apps.twitter.com and look up your Twitter API Credentials, or create an app to create them.
* Enter your credentials into the cell below

```scala
System.setProperty("twitter4j.oauth.consumerKey", "CONSUMER_API_KEY")
System.setProperty("twitter4j.oauth.consumerSecret", "CONSUMER_API_SECRET")
System.setProperty("twitter4j.oauth.accessToken", "ACCESS_TOKEN")
System.setProperty("twitter4j.oauth.accessTokenSecret", "ACCESS_TOKEN_SECRET")
```

## Step 2: Configure where to output the top hashtags and how often to compute them.
* Run the below cell to configure output directory and computation frequency
* Feel free to adjust these and experiment with different values

```scala
// Directory to output top hashtags
val outputDirectory = "/twitter"

// Recompute the top hashtags every 1 second
val slideInterval = new Duration(1 * 1000)

// Compute the top hashtags for the last 5 seconds
val windowLength = new Duration(5 * 1000)

// Wait this many seconds before stopping the streaming job
val timeoutJobLength = 100 * 1000
```

## Step 3: Run the Twitter Streaming job.
* Clean up any old files.

```scala
dbutils.fs.rm(outputDirectory, true)
```

* Create the function to that creates the Streaming Context and sets up the streaming job.

```scala
var newContextCreated = false
var num = 0

// This is a helper class used for 
object SecondValueOrdering extends Ordering[(String, Int)] {
  def compare(a: (String, Int), b: (String, Int)) = {
    a._2 compare b._2
  }
}

// This is the function that creates the SteamingContext and sets up the Spark Streaming job.
def creatingFunc(): StreamingContext = {
  // Create a Spark Streaming Context.
  val ssc = new StreamingContext(sc, slideInterval)
  // Create a Twitter Stream for the input source. 
  val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
  val twitterStream = TwitterUtils.createStream(ssc, auth)
  
  // Parse the tweets and gather the hashTags.
  val hashTagStream = twitterStream.map(_.getText).flatMap(_.split(" ")).filter(_.startsWith("#"))
  
  // Compute the counts of each hashtag by window.
  val windowedhashTagCountStream = hashTagStream.map((_, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x + y, windowLength, slideInterval)

  // For each window, calculate the top hashtags for that time period.
  windowedhashTagCountStream.foreachRDD(hashTagCountRDD => {
    val topEndpoints = hashTagCountRDD.top(10)(SecondValueOrdering)
    dbutils.fs.put(s"${outputDirectory}/top_hashtags_${num}", topEndpoints.mkString("\n"), true)
    println(s"------ TOP HASHTAGS For window ${num}")
    println(topEndpoints.mkString("\n"))
    num = num + 1
  })
  
  newContextCreated = true
  ssc
}
```

* Create the StreamingContext using getActiveOrCreate, as required when starting a streaming job in Databricks.
 
```scala
@transient val ssc = StreamingContext.getActiveOrCreate(creatingFunc)

```

* Start the Spark Streaming Context and return when the Streaming job exits or return with the specified timeout.

```scala
ssc.start()
ssc.awaitTerminationOrTimeout(timeoutJobLength)
```

* Stop any active Streaming Contexts, but don't stop the spark contexts they are attached to.
 
```scala
StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }

```

## Step 4: View the Results.

```scala
display(dbutils.fs.ls(outputDirectory))
```

```scala
dbutils.fs.head(s"${outputDirectory}/top_hashtags_3")
```
