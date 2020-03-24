package com.github.coiacob.kafka.tutorial1.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

// #1
public class ProducerDemo {

    public static void main(String[] args) {

        String bootstrapServers = "localhost:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Key and value serializer help the producer know what type of value are you sending to kafka
        // and how should be this serialized to bytes
        //  Kafka client will convert whatever we send into bytes
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String>("first_topic", "hello world");

        // send data -asynchronous, so this it will happen in the background,
        // as this is executed, the program exists and the data never sends
        producer.send(producerRecord);

        // flush data
        // to wait for the data to be produced (forces all data to be produced)
        producer.flush();

        // flush and close producer
        producer.close();
    }
}