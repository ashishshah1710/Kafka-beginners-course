package io.conduktor.demos.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field.Str;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemokeys {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemokeys.class.getSimpleName());
    public static void main(String[] args) {
      LOGGER.info("I am a kafka producerCallBack");

      //create Producer properties
      Properties properties = new Properties();
      properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

      //set Producer properties
      properties.setProperty("key.serializer", StringSerializer.class.getName());
      properties.setProperty("value.serializer", StringSerializer.class.getName());
      //properties.setProperty("batch.size", "400");
      //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

      //create the producer
      KafkaProducer<String,String> producer = new KafkaProducer<>(properties);


        for (int i = 0; i < 10; i++) {
          //create a producer Record
          String topic = "demo_java_topic";
          String key = "id_" + i;
          String value = "Hello World" + i;
          ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
              topic, key, value);

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
                    "Timestamp: " + recordMetadata.timestamp() + "\n" +
                    "Key: "+ key);
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


      //flush data
      producer.flush();

      //close producer
      producer.close();
    }
}
