package com.xiaoniuhy.adp.tracking_streaming

import scala.collection.mutable.ListBuffer
import java.util.ArrayList;
import scala.collection.JavaConverters._
import java.text.SimpleDateFormat 

import scala.collection.immutable.StringLike


import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.ByteBufferDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import java.nio.ByteBuffer

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.xiaoniuhy.adp.thrift.EventMergeService

import com.xiaoniuhy.adp.pb.tracking.TrackingLog
import com.xiaoniuhy.adp.pb.EventType
import com.xiaoniuhy.adp.pb.tracking.BidInfo
import com.xiaoniuhy.adp.pb.clickhouse.AdpTrackingLogEvent
import com.xiaoniuhy.adp.pb.clickhouse.AdpDeviceType
import com.xiaoniuhy.adp.pb.clickhouse.AdpNetworkType
import com.xiaoniuhy.adp.pb.clickhouse.AdpGeoType
import com.xiaoniuhy.adp.pb.clickhouse.AdpSlotType
import com.xiaoniuhy.adp.pb.clickhouse.AdpBidType
import com.xiaoniuhy.adp.pb.clickhouse.AdpTimeType
import com.xiaoniuhy.adp.pb.clickhouse.AdpEventType


object main {

     def sendBatchClient(trackingEvents: List[AdpTrackingLogEvent] ):AdpTrackingLogEvent =  {
        var event:AdpTrackingLogEvent = null;
        var tTransport:TTransport = null;
        try {
            tTransport = new TSocket("localhost", 8989, 30000);
            // 协议要和服务端一致
            var protocol = new TBinaryProtocol(tTransport);
            var client = new EventMergeService.Client(protocol);
            tTransport.open();
            var trackingEventsBytes = new ArrayList[ByteBuffer]();
            for(tmp <- trackingEvents){
                trackingEventsBytes.add( ByteBuffer.wrap(tmp.toByteArray()));
            }
            client.batchEvent( "test",trackingEventsBytes);
        } catch{
          case  ex:TException =>{
            print(" InvalidProtocolBufferException");
            ex.printStackTrace();
          }
        }
        finally {
            if (tTransport != null) {
                tTransport.close();
            }
        }
        return event;
    }



  def mergeTime(builder:AdpTimeType.Builder, trackingLog: TrackingLog)={
      builder.setTimestamp(trackingLog.getEventTime())
  }


  def mergeDeivce(builder:AdpDeviceType.Builder, bidInfo: BidInfo)={
      builder.setOs(bidInfo.getOs().toString())
      builder.setOsVersion(bidInfo.getOsv())
      builder.setBrand(bidInfo.getBrand())
      builder.setModel(bidInfo.getModel())
  }

  def mergeNetwork(builder:AdpNetworkType.Builder, bidInfo: BidInfo)={
      builder.setConnection(bidInfo.getConn().toString())
      builder.setOperator(bidInfo.getOperator())
      builder.setIp(bidInfo.getIp())
      //builder.setCarrier(bidInfo.getCarrier())
  }

  def mergeGeo(builder:AdpGeoType.Builder, bidInfo: BidInfo)={
      builder.setLatitude(bidInfo.getLat())
      builder.setLongitude(bidInfo.getLon())
      // builder.setCity(bidInfo.getCity())
      // builder.setProvince(bidInfo.getProvince())
  }

  def mergeSlot(builder:AdpSlotType.Builder, bidInfo: BidInfo)={
      builder.setImpType(bidInfo.getImpType())
      builder.setActionType(bidInfo.getActionType())
  }

  def mergeBid(builder:AdpBidType.Builder, bidInfo: BidInfo)={
      val xn_bi = bidInfo.getXnBi()
      builder.setCompanyId(xn_bi.getAdvid())
      builder.setCampaignId(xn_bi.getCampid())
      //builder.setPlanid(xn_bi.getPlanid())
  }

def matchEventCode(x: Int): AdpEventType = x match {
      case EventType.EVENT_IMP_VALUE => AdpEventType.Impression
      case EventType.EVENT_CLICK_VALUE => AdpEventType.Click
      case _ => AdpEventType.UNRECOGNIZED
      
   }

  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local[2]") 
    .setAppName("NetworkWordCount") 
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    val ssc = new StreamingContext(conf, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "172.16.11.1:9092,172.16.11.252:9092,172.16.11.89:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[ByteBufferDeserializer],
      "group.id" -> "test_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("adp_test")
    val stream = KafkaUtils.createDirectStream[String, ByteBuffer](
      ssc,
      PreferConsistent,
      Subscribe[String, ByteBuffer](topics, kafkaParams)
    )
  stream.print()
    //stream.map(record => print((record.key, record.value)))

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
       var arrayRows = new ListBuffer[AdpTrackingLogEvent]();
        for(  x <- iter ){
          val log = TrackingLog.parseFrom( x.value())
          var builder = AdpTrackingLogEvent.newBuilder()
          builder.setRequestId(log.getBidInfo().getReqId().toString())
          val eventCode = log.getEventCode()
          builder.setEventType(matchEventCode(eventCode))
          mergeTime(builder.getTimeBuilder(),log)
          mergeDeivce(builder.getDeviceBuilder(),log.getBidInfo())
          mergeNetwork(builder.getNetworkBuilder(),log.getBidInfo())
          mergeGeo(builder.getGeoBuilder(),log.getBidInfo())
          mergeSlot(builder.getSlotBuilder(),log.getBidInfo())
          mergeBid(builder.getBidBuilder(),log.getBidInfo())
          val row =builder.build()
          arrayRows += row
        }
        if(arrayRows.length != 0){
          sendBatchClient(arrayRows.toList)
        }

        
      }
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()
  }
}
