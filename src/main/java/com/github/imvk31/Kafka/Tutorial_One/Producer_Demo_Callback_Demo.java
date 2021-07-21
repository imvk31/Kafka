package com.github.imvk31.Kafka.Tutorial_One;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.ProducerBatch;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer_Demo_Callback_Demo
{
    public static void main(String[] args)
    {
        String boostrapServers = "localhost:9092";
        Logger logger = LoggerFactory.getLogger(Producer_Demo_Callback_Demo.class);
        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
    for(int i=0; i<10; i++) {
        //Create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "This is Varam"+Integer.toString(i));

        //Send Data - Ascynchronous
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //Executes every time record is successfully sent
                if (e == null) {
                    logger.info("Received new Data. \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partitions: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("error while producing ", e);
                }
            }
        });
        producer.flush();
    }

    }
}
