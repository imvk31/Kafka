package com.github.imvk31.Kafka.Tutorial_One;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer_Demo_Keys
{
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String boostrapServers = "localhost:9092";
        Logger logger = LoggerFactory.getLogger(Producer_Demo_Keys.class);

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0; i<6; i++)
        {
            String topic = "first_topic";
            String value = "Hello World "+Integer.toString(i);
            String key = "id_"+Integer.toString(i);
        //Create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value);
        logger.info(key);
            //id_0 => Partition 1
            //id_1 => Partition 0
            //id_2 => Partition 2
            //id_3 => Partition 0
            //id_4 => Partition 2
            //id_5 => Partition 2
            //id_6 => Partition 0
            //id_7 => Partition 2
            //id_8 => Partition 1
            //id_0 => Partition 2

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
        }).get();  //blocked the .send() to make it synchronous

        producer.flush();
    }
    
    }
}
