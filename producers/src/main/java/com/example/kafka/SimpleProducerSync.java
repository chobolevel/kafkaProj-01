package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleProducerSync {

    private static final Logger logger = LoggerFactory.getLogger(SimpleProducerSync.class);

    public static void main(String[] args) {

        String topicName = "simple-topic";
        // kafkaProducer 객체 Config 작성
        // null: "hello world" 메시지 보낼 예정

        Properties props = new Properties();
        // bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // 위와 아래는 같은 설정을 의미
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // KafkaProducer 객체 생성
        // 생성자 파라미터로 설정정보를 받음
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        
        // ProducerRecord 객체 생성
        // 생성자 파라미터로 토픽이름, key, value를 받음
        // key값을 생략하는 생성자도 있음
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName,"hello world3");
        
        // kafkaProducer 메세지 전송
        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("\n ###### record metadata received ##### \n" +
                    "partitions : " + recordMetadata.partition() + "\n" +
                    "offset : " + recordMetadata.offset() + "\n" +
                    "timestamp : " + recordMetadata.timestamp());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }


    }
}
