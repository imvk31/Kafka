package com.github.imvk31.Kafka.Tutorial_One;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer_Demo_1
{
    public static void main(String[] args)
    {
        String bootstrap_server1 = "localhost:9092";

        //Producer Properties
        Properties properties1 = new Properties();
        properties1.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server1);
        properties1.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties1.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties1);

        //Create Producer Record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "This from the other side");

        //Send the Record
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
