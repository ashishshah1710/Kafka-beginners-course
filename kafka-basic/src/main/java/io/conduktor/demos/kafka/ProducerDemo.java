package io.conduktor.demos.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
      LOGGER.info("I am a kafka producer");

      //create Producer properties
      Properties properties = new Properties();
      properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

      //set Producer properties
      properties.setProperty("key.serializer", StringSerializer.class.getName());
      properties.setProperty("value.serializer", StringSerializer.class.getName());

      //create the producer
      KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

      //create a producer Record
      ProducerRecord<String,String>producerRecord = new ProducerRecord<>("demo_java_first_topic", "Hello World");

      //send data
      producer.send(producerRecord);

      //flush data
      producer.flush();

      //close producer
      producer.close();
    }
}
