package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class SimpleConsumer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);

    public static void main(String[] args) {

        String topicName = "simple-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "simple-group");
        // Heart Beat Thread가 Heart Beat를 보내는 간격
        props.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "5000");
        // 브로커가 Consumer로부터 Heart Beat 응답을 기다리는 시간
        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "90000");
        // poll호출 후 다음 poll까지 브로커가 기다리는 시간(오버하는 경우 Consumer와 연결이 끊기고 Rebalance)
        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of(topicName));

        while(true) {
            // poll 메서드 수행은 Poll Thread가 수행 Main Thread X
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String, String> record : records) {
                logger.info("record key: {}, record value: {}, partition: {}", record.key(), record.value(), record.partition());
            }
        }

        // close()

    }
}
