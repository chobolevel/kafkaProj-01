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

public class PizzaProducer {

    private static final Logger logger = LoggerFactory.getLogger(PizzaProducer.class);

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

        String topicName = "pizza-topic";
        // kafkaProducer 객체 Config 작성
        // null: "hello world" 메시지 보낼 예정

        Properties props = new Properties();
        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // 위와 아래는 같은 설정을 의미
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.setProperty(ProducerConfig.ACKS_CONFIG, "0");
        
        // KafkaProducer 객체 생성
        // 생성자 파라미터로 설정정보를 받음
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        sendPizzaMessage(kafkaProducer, topicName, -1, 10, 100, 100, true);

        kafkaProducer.close();

    }
}
