package com.twitter.app
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.streaming.{ StreamingContext, Seconds }
import org.apache.spark.streaming.kafka010.{ ConsumerStrategies, KafkaUtils, LocationStrategies }
import org.apache.spark.streaming.dstream.DStream
import scala.io.Source
import org.apache.log4j.{ Level, Logger }
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }
import org.apache.kafka.common.serialization.{ StringDeserializer, StringSerializer }
import org.apache.spark.rdd.RDD
import TweetsAnalysis._

object TweetsConsumer {

  val conf = new SparkConf()
    .set( "es.nodes", "localhost" )
    .set( "es.port", "9200" )
    .set( "es.index.auto.create", "true" )

  val spark = SparkSession.builder()
    .appName( "Twitter Sentiment Analysis" )
    .master( "local[*]" )
    .config( conf )
    .getOrCreate()

  val ssc = new StreamingContext( spark.sparkContext, Seconds( 10 ) )

  val kafkaParams : Map[ String, Object ] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> classOf[ StringSerializer ],
    "value.serializer" -> classOf[ StringSerializer ],
    "key.deserializer" -> classOf[ StringDeserializer ],
    "value.deserializer" -> classOf[ StringDeserializer ],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[ Object ],
    "group.id" -> "group1"
  )

  val kafkaTopic = "twitter"

  def setupLogging() = {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel( Level.ERROR )
  }

  def readStreamFromKafka() : DStream[ String ] = {
    val topics = Array( kafkaTopic )
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[ String, String ]( topics, kafkaParams )
    )
    val processedStream = kafkaDStream.map( record ⇒ record.value() )
    processedStream
  }

  def processRDD( rdd : RDD[ String ] ) : Unit = {
    if ( !rdd.isEmpty() ) processTwitterDF( spark, convertJsonRDDtoDF( rdd ) )
    else println( "No data is streaming. Waiting for next batch." )
  }

  def convertJsonRDDtoDF( rdd : RDD[ String ] ) : DataFrame = {
    println( "iterating batch of data." )
    val rawStreamDF = spark.read
      .option( "multiLine", true )
      .option( "mode", "DROPMALFORMED" )
      .json( rdd )
    rawStreamDF
  }

  def startStream() : Unit = {
    setupLogging()
    val stream : DStream[ String ] = readStreamFromKafka()
    stream foreachRDD { rdd ⇒ processRDD( rdd ) }
    ssc.start()
    ssc.awaitTermination()
  }

}
