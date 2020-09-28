package com.twitter.app

import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._

object TweetsWriter {

  val FS_OUTPUT_LOCATION = "file:///home/hduser/twitter/results/output.csv"
  val HDFS_OUTPUT_LOCATION = "/user/hduser/twitter/results/output.csv"
  val HIVE_OUTPUT_TABLE = "twitterdb.tweets"
  val MYSQL = Map(
    "username" → "root",
    "password" → "root",
    "jdbcUrl" → "jdbc:mysql://localhost/twitter",
    "tablename" → "tweetsdata"
  )
  val ES_INDEX = "twitter/tweetsdata"
  val KAFKA = Map(
    "consumer_topic" → "tweet_consumer",
    "bootstrap_server" → "localhost:9092"
  )

  def writeToFS( finalDF : DataFrame ) : Unit = {
    finalDF
      .write
      .format("csv")
      .option( "header", "true" )
      .mode( "append" )
      .save( FS_OUTPUT_LOCATION )
    println( "tweets written to File System." )
  }

  def writeToHDFS( finalDF : DataFrame ) : Unit = {
    finalDF
      .write
      .format("csv")
      .option( "header", "true" )
      .mode( "append" )
      .save( HDFS_OUTPUT_LOCATION )
    println( "tweets written to HDFS." )
  }

  def writeToHive( finalDF : DataFrame ) : Unit = {
    finalDF
      .write
      .mode( "append" )
      .saveAsTable( HIVE_OUTPUT_TABLE )
    println( "tweets written to Hive." )
  }

  def writeToMySQL( finalDF : DataFrame ) : Unit = {
    val prop = new java.util.Properties();
    prop.put( "user", MYSQL( "username" ) )
    prop.put( "password", MYSQL( "password" ) )
    finalDF
    .write
    .mode( "append" )
    .jdbc( MYSQL( "jdbcUrl" ), MYSQL( "tablename" ), prop )
    println( "tweets written to MySQL." )
  }

  def writeToES( finalDF : DataFrame ) : Unit = {
    finalDF.saveToEs( ES_INDEX )
    println( "tweets written to Elastic Search." )
  }

  def writeToKafka( finalDF : DataFrame ) : Unit = {
    finalDF.
      selectExpr( "to_json(struct(*)) as value" )
      .write
      .format( "kafka" )
      .option( "topic", KAFKA( "consumer_topic" ) )
      .option( "kafka.bootstrap.servers", KAFKA( "bootstrap_server" ) )
      .save()
    println( "tweets published to Kakfa Topic." )
  }

}
