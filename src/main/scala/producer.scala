package main

import com.google.protobuf.ByteString

import scrooge.scrooge_message._
import scrooge.scrooge_networking._
import scrooge.scrooge_request._

import org.apache.kafka.clients.producer._
import java.util.Properties
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.io.RandomAccessFile

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

object Producer {
  val configReader = new ConfigReader
  val rsmId = configReader.getRsmId()
  val nodeId = configReader.getNodeId()
  val topic = if (rsmId == 1) configReader.getTopic1() else configReader.getTopic2()
  val brokerIps = configReader.getBrokerIps()
  val rsmSize = configReader.getRsmSize()
  val benchmarkDuration = configReader.getBenchmarkDuration()
  val warmupDuration = configReader.getWarmupDuration()
  val cooldownDuration = configReader.getCooldownDuration()
  val inputPath = configReader.getInputPath() // Path to Linux pipe

  def main(args: Array[String]): Unit = {
    if (topic == "") {
      return;
    }

    println("STARTING PRODUCER")
    println(warmupDuration)
    println("^^^ warmupduration")
    // Warmup period
    val warmup = warmupDuration.seconds.fromNow
    while (warmup.hasTimeLeft()) { } // Do nothing 
    val produceMessages = Future { // Run on a separate thread
      writeToKafka()
    }
    Await.result(produceMessages, Duration.Inf) // Wait on new thread to finish

  }

  def writeToKafka(): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", brokerIps) // To test locally, change brokerIps with "localhost:9092"
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("acks", "1")
    props.put("linger.ms", 5)
    props.put("batch.size", 100000)
    props.put("buffer.memory", 1000000000)

    val producer = new KafkaProducer[String, Array[Byte]](props)
    if (configReader.shouldReadFromPipe()) { // Send message from Linux pipe
      val linuxPipe = new RandomAccessFile(inputPath, "r")
      if (linuxPipe != null) {
        // println("Pipe exists!")
      }
      val linuxChannel = linuxPipe.getChannel

      val sizeBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
      val protobufStrBuffer = ByteBuffer.allocate(8388608).order(ByteOrder.LITTLE_ENDIAN)
      
      val timer = benchmarkDuration.seconds.fromNow
      
      var messagesSerialized = 0
      var startTime = System.currentTimeMillis()

      println("starting timer")
      while (timer.hasTimeLeft()) {

        sizeBuffer.clear()
        protobufStrBuffer.clear()
        var sizeBufferBytesRead = 0
        while (sizeBufferBytesRead < 8) { 
          sizeBufferBytesRead += linuxChannel.read(sizeBuffer)
        }

        sizeBuffer.flip()
        val protobufStrSize = sizeBuffer.getLong
        // println("read number of bytes")

        protobufStrBuffer.limit(protobufStrSize.toInt)
        var protobufStrBufferBytesRead = 0
        while (protobufStrBufferBytesRead < protobufStrSize) {
          protobufStrBufferBytesRead += linuxChannel.read(protobufStrBuffer)
         }

        protobufStrBuffer.flip()
        val protobufStrBytes = new Array[Byte](protobufStrSize.toInt)
        protobufStrBuffer.get(protobufStrBytes)
        val protobufStr = new String(protobufStrBytes)     
        // println("read the string from raft")   
        
        val scroogeReq = ScroogeRequest.parseFrom(protobufStrBytes)
        val maybeCrossChainMessageData = scroogeReq.request match {
          case ScroogeRequest.Request.SendMessageRequest(sendMessageRequest) => 
            // println("found content within raft's pipe")
            Some(sendMessageRequest.content)
          case _ => 
            None
        }

        maybeCrossChainMessageData match {
          case Some(v) =>
            val crossChainMessageData = v.get
            if (crossChainMessageData.sequenceNumber % rsmSize == nodeId) {
              println(s"Sending message with content: ${crossChainMessageData.messageContent}")
              val crossChainMessage = CrossChainMessage (
                data = Seq(crossChainMessageData)
              )
              val seralizedMesage = crossChainMessage.toByteArray

              val record = new ProducerRecord[String, Array[Byte]](topic, nodeId.toInt, nodeId.toInt.toString(), seralizedMesage)
              producer.send(record)
              messagesSerialized += 1
            }
            
          case None =>
            println("CrossChainMessageData not found")
        }
      }

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> bc41b35 (Added log to see producer throughput)
      val finalTime = System.currentTimeMillis()
      val overallThroughput = messagesSerialized.toDouble / ((finalTime - startTime).toDouble/1000)
      println("Overall Throughput: " + overallThroughput)

<<<<<<< HEAD
      println("before closing producer pipe")
=======
=======
>>>>>>> bc41b35 (Added log to see producer throughput)
      // println("before closing producer pipe")
>>>>>>> d570fa0 (no print lines)
      linuxPipe.close()
      linuxChannel.close()
      // println("after closing producer pipe")

    } else { // Send message from config
      val timer = benchmarkDuration.seconds.fromNow
      
      var messagesSerialized = 0
      var startTime = System.currentTimeMillis()
    
      println("starting timer")
      while (timer.hasTimeLeft()) {
        val messageStr = configReader.getMessage()
        val messageStrBytes = messageStr.getBytes("UTF-8")
        val messageData = CrossChainMessageData (
          messageContent = ByteString.copyFrom(messageStrBytes)
          // Optionally add any other attributes (e.g. sequenceNumber)
        )
  
        val crossChainMessage = CrossChainMessage (
          data = Seq(messageData)
        )
        val seralizedMesage = crossChainMessage.toByteArray
        println(s"Sending message with content: ${messageData.messageContent}") 
        val record = new ProducerRecord[String, Array[Byte]](topic, nodeId.toInt, nodeId.toInt.toString(), seralizedMesage)
        producer.send(record)
        messagesSerialized += 1
      }

      val finalTime = System.currentTimeMillis()
      val overallThroughput = messagesSerialized.toDouble / ((finalTime - startTime).toDouble/1000)
      println("Overall Throughput: " + overallThroughput)
<<<<<<< HEAD
      
      println("Producer Metrics: ")
      producer.metrics().entrySet().forEach(x => println("key=" + x.getKey().name() + ",value=" + x.getValue().metricValue().toString()))
=======
>>>>>>> bc41b35 (Added log to see producer throughput)
    }

    producer.close()
  }
}
