package com.example.kafka;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class PizzaProducerCustomPartitioner {

    private static final Logger logger = LoggerFactory.getLogger(PizzaProducerCustomPartitioner.class);

    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
                                        String topicName,
                                        int interCount,
                                        int interIntervalMillis,
                                        int intervalMillis,
                                        int intervalCount,
                                        boolean sync) {
        PizzaMessage pizzaMessage = new PizzaMessage();
        int iterSeq = 0;
        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        while(iterSeq++ != interCount) {
            HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, pMessage.get("key"), pMessage.get("message"));
            sendMessage(kafkaProducer, producerRecord, pMessage, sync);
            if((intervalCount > 0) && (iterSeq % intervalCount == 0)) {
                try {
                    logger.info("##### IntervalCount: " + intervalCount + " intervalMillis: " + intervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
            if(interIntervalMillis > 0) {
                try {
                    logger.info("interIntervalMillis: " + interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

    public static void sendMessage(KafkaProducer<String, String> kafkaProducer, ProducerRecord<String, String> producerRecord, HashMap<String, String> pMessage, boolean sync) {
        // 비동기 전송
        if(!sync) {
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if(exception == null) {
                    logger.info("async message: " + pMessage.get("key") + "partitions: " + metadata.partition() +
                            "offset: " + metadata.offset());
                } else {
                    logger.error("exception error from broker = {}", exception.getMessage());
                }
            });
        } else {
            // 동기 전송
            try {
                RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
                logger.info("sync message: " + pMessage.get("key") + " partition: " + metadata.partition() +
                        "offset: " + metadata.offset());
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            } catch (ExecutionException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public static void main(String[] args) {

        String topicName = "pizza-topic-partitioner";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty("custom.specialKey", "P001");
        // 같은 설정
        // 같은 패키지의 경우 클래스명만 넘겨도 됨
        props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.example.kafka.CustomPartitioner");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        sendPizzaMessage(kafkaProducer, topicName, -1, 100, 0, 0, true);

        kafkaProducer.close();

    }
}
