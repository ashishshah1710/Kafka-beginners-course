package io.conduktor.demos.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallBack {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithCallBack.class.getSimpleName());
    public static void main(String[] args) {
      LOGGER.info("I am a kafka producerCallBack");

      //create Producer properties
      Properties properties = new Properties();
      properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

      //set Producer properties
      properties.setProperty("key.serializer", StringSerializer.class.getName());
      properties.setProperty("value.serializer", StringSerializer.class.getName());
      properties.setProperty("batch.size", "400");
      //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

      //create the producer
      KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

      for(int j=0;j<10;j++) {
        for (int i = 0; i < 30; i++) {
          //create a producer Record
          ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
              "demo_java_first_topic", "Hello World" + i);

          //send data
          producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
              // executes every time a record is successfully sent or an exception is thrown
              if (e == null) {
                //the record was successfully sent
                LOGGER.info("Received new metadata \n" +
                    "Topic: " + recordMetadata.topic() + "\n" +
                    "Partition: " + recordMetadata.partition() + "\n" +
                    "Offset: " + recordMetadata.offset() + "\n" +
                    "Timestamp: " + recordMetadata.timestamp() + "\n");
              } else {
                LOGGER.error("Error while producing", e);
              }
            }
          });
        }
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      //flush data
      producer.flush();

      //close producer
      producer.close();
    }
}
