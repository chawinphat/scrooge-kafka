import java.util.Properties
import org.apache.kafka.clients.producer._

object Producer {

  def main(args: Array[String]): Unit = {
    writeToKafka("quickstart-events", "helloo")
  }

  def writeToKafka(topic: String, message: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic, message)
    producer.send(record)
    producer.close()
  }
}
