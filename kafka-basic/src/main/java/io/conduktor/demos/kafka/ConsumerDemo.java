package io.conduktor.demos.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {
      LOGGER.info("I am a kafka Consumer");

      String groupId = "my-first-application";
      String topic = "demo_java_first_topic";
      //create Consumer properties
      Properties properties = new Properties();
      properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

      //set Consumer properties
      properties.setProperty("key.deserializer", StringDeserializer.class.getName());
      properties.setProperty("value.deserializer", StringDeserializer.class.getName());
      properties.setProperty("group.id", groupId);
      properties.setProperty("auto.offset.reset", "earliest");

      //create the consumer
      KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

      //subscribe to a topic
      kafkaConsumer.subscribe(Arrays.asList(topic));

      //poll for new data
      while (true) {
        LOGGER.info("Polling for new data");
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

        for(ConsumerRecord<String,String> record : records){
          LOGGER.info("Key: " + record.key() + ", Value: " + record.value());
          LOGGER.info("partition: "+record.partition() + ", offset: " + record.offset());
        }
      }
    }
}
