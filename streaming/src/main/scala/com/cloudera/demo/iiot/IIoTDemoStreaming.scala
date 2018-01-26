package com.cloudera.demo.iiot

import java.io.ByteArrayInputStream
import java.util.HashMap
import java.util.zip.GZIPInputStream

import scala.collection.JavaConverters._
import scala.util.Try

import com.trueaccord.scalapb.spark._

import kafka.serializer._
import org.apache.log4j.{Level, Logger}
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{StreamingContext, Seconds}

import org.eclipse.kapua.service.device.call.message.kura.proto.kurapayload.KuraPayload

object IIoTDemoStreaming {

  // TEMPORARY WORKAROUND UNTIL DEV BOARD TIMESTAMP ISSUE RESOLVED
  //case class Telemetry(id:String, millis:Option[Long], metric:String, value:Option[String])
  case class Telemetry(id:String, millis:Long, metric:String, value:Option[String])

  def main(args:Array[String]):Unit = {

    // Usage
    if (args.length == 0) {
      println("Args: <kafka-broker-list e.g. broker-1:9092,broker-2:9092> <kudu-master e.g. kudu-master-1:7051,kudu-master-2:7051>")
      return
    }

    // Args
    val kafkaBrokerList = args(0)
    val kuduMasterList = args(1)

    // Hardcoded params
    val kafkaTopicIn = "telemetry"
    val kuduTelemetryTable = "impala::iiot.telemetry"

    // Configure app
    val sparkConf = new SparkConf().setAppName("IIoTDemoStreaming")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(3))
    val sqc = new SQLContext(sc)
    val kc = new KuduContext(kuduMasterList)

    import sqc.implicits._

    // Consume messages from Kafka
    val kafkaConf = Map[String,String](
      "metadata.broker.list" -> kafkaBrokerList,
      "group.id" -> "IIoTDemoStreamingApp"
    )
    val kafkaDStream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaConf, Set(kafkaTopicIn))

    // Parse raw messages values into protobuf objects
    val kurapayloadDStream = kafkaDStream.map(message => {
      val key = message._1

      val value:KuraPayload = {
        val tryUnzipAndParse = Try { KuraPayload.parseFrom(new GZIPInputStream(new ByteArrayInputStream(message._2))) }
        if (tryUnzipAndParse.isSuccess) {
          tryUnzipAndParse.get
        }
        else {
          KuraPayload.parseFrom(message._2)
        }
      }

      (key, value)
    })

    kurapayloadDStream.print()

    // Convert (id, KuraPayload) tuple DStream into Telemetry DStream
    val telemetryDStream = kurapayloadDStream.flatMap(message => {
      val id = message._1
      // TEMPORARY WORKAROUND UNTIL DEV BOARD TIMESTAMP ISSUE RESOLVED
      //val millis = message._2.timestamp
      val millis = System.currentTimeMillis()

      val metricsList = message._2.metric


      // TEMPORARY WORKAROUND UNTIL DEV BOARD TIMESTAMP ISSUE RESOLVED
      //var telemetryArray = new Array[Telemetry](metricsList.length)
      var telemetryArray = new Array[Telemetry](metricsList.length / 2)

      var i = 0
      for (metric <- metricsList) {
        val metricValue = {
          // Convert all metrics to String
          if (metric.`type`.isDouble) metric.doubleValue.map(_.toString)
          else if (metric.`type`.isFloat) metric.floatValue.map(_.toString)
          else if (metric.`type`.isInt64) metric.longValue.map(_.toString)
          else if (metric.`type`.isInt32) metric.intValue.map(_.toString)
          else if (metric.`type`.isBool) metric.boolValue.map(_.toString)
          else if (metric.`type`.isBytes) metric.bytesValue.map(_.toString)
          else metric.stringValue

        }
        // TEMPORARY WORKAROUND UNTIL DEV BOARD TIMESTAMP ISSUE RESOLVED
        //telemetryArray(i) = new Telemetry(id, millis, metric.name, metricValue)
        //i += 1
        if (!metric.name.endsWith("_timestamp")) {
          telemetryArray(i) = new Telemetry(id, millis, metric.name, metricValue)
          i += 1
        }

      }

      telemetryArray
    })

    telemetryDStream.print()

    // Convert each Telemetry RDD in the Telemetry DStream into a DataFrame and insert to Kudu
    telemetryDStream.foreachRDD(rdd => {
      val telemetryDF = rdd.toDF()

      kc.insertRows(telemetryDF, kuduTelemetryTable)
    })

    ssc.checkpoint("/tmp/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
