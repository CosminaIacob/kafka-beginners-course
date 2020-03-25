package com.github.coiacob.kafka.tutorial1.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

// #4
public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        String bootstrapServers = "localhost:9092";
        String topic = "first_topic";

        // create consumer configs (properties)
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // earliest - read from the beginning of the topic
        // latest - read only the new messages onwards
        // none - throw an error if there are no offsets being saved
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // assign and seek - are mostly used to replay data or reach a specific message

        // assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Collections.singletonList(partitionToReadFrom));

        //seek
        long offsetsToReadFrom = 15L;
        consumer.seek(partitionToReadFrom, offsetsToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        // poll for new data
        while (keepOnReading) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar += 1;
                logger.info("Key: " + record.key() + "\n" +
                        "Value: " + record.value() + "\n" +
                        "Partition: " + record.partition() + "\n" +
                        "Offset: " + record.offset() + "\n");
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false; // to exit the while loop
                    break; // to exist the for loop
                }
            }
        }
        logger.info("Exiting th application");
    }
}
