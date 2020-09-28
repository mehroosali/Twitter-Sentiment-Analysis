package com.twitter.app
import com.twitter.util.SentimentUtil
import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.functions._

object TweetsAnalysis {

  def processTwitterDF( spark : SparkSession, tweetDF : DataFrame ) : Unit = {

    tweetDF.createOrReplaceTempView( "twitterstream" )
    spark.udf.register( "detectsentiment", SentimentUtil.detectSentiment _ )

    val tweetResultDF = spark.sql( """
      select
             created_at,
             lang,
             place.country as country,
             user.screen_name as user_name,
             text as tweet,
             retweet_count,
             possibly_sensitive,
             detectsentiment(text) as sentiment
      from twitterstream
      """ )

    tweetResultDF.show()

    TweetsWriter.writeToES( tweetResultDF )

  }

}
