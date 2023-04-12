package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerWakeupV2 {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerWakeupV2.class);

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_02");
        // poll 메서드 간격 최대 60초 기다림
        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of(topicName));

        // main Thread 참조 변수
        Thread mainThread = Thread.currentThread();

        // main Thread 종료시 별도의 Thread로 kafkaConsumer의 wakeup() 메서드 호출
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("main program starts to exit by calling wakeup");
                kafkaConsumer.wakeup();
                try {
                    // main Thread가 종료되기 현재 Thread 유지하기 위함
                    // main Thread 종료되기 전에 실행되는 듯
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        int loopCnt = 0;
        try {
            while(true) {
                // poll 메서드 수행은 Poll Thread가 수행 Main Thread X
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info("############ loopCnt: {}, ConsumerRecord count:{}", loopCnt++, records.count());
                for(ConsumerRecord<String, String> record : records) {
                    logger.info("record key: {}, partition: {}, record offset: {}, record value: {} ", record.key(), record.partition(), record.offset(),  record.value());
                }
                try {
                    logger.info("main thread is sleeping {} ms during while sleep", loopCnt * 10000);
                    Thread.sleep(loopCnt * 10000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (WakeupException e) {
            // consumer가 종료된 경우 발생하는 예외
            logger.error("wakeup exception has been called");
        } finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }


        // close()

    }
}
