package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerPartitionAssign {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerPartitionAssign.class);

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_pizza_assign_seek");
//        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "6000");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
//        특정 파티션 사용하는 경우 subscribe() 메서드 사용하지 않음
//        kafkaConsumer.subscribe(List.of(topicName));
        kafkaConsumer.assign(Arrays.asList(topicPartition));

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

//        pollAutoCommit(kafkaConsumer);

        pollCommitSync(kafkaConsumer);

//        pollCommitAsync(kafkaConsumer);


    }

    private static void pollAutoCommit(KafkaConsumer<String, String> kafkaConsumer) {
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
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (WakeupException e) {
            // consumer가 종료된 경우 발생하는 예외
            logger.error("wakeup exception has been called");
        } finally {
            // 혹시나를 대비
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }
    }

    private static void pollCommitSync(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 0;
        try {
            while(true) {
                // poll 메서드 수행은 Poll Thread가 수행 Main Thread X
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info("############ loopCnt: {}, ConsumerRecord count:{}", loopCnt++, records.count());
                for(ConsumerRecord<String, String> record : records) {
                    logger.info("record key: {}, partition: {}, record offset: {}, record value: {} ", record.key(), record.partition(), record.offset(),  record.value());
                }
                // 동기 수동 commit
                // commit은 메세지 단위로 하지 않음 배치로 읽어오기 때문(할 수는 있음)
                try {
                    if(records.count() > 0) {
                        kafkaConsumer.commitSync();
                        logger.info("commit sync has been called");
                    }
                } catch (CommitFailedException e) {
                    logger.error(e.getMessage());
                }
            }
        } catch (WakeupException e) {
            // consumer가 종료된 경우 발생하는 예외
            logger.error("wakeup exception has been called");
        } finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }
    }

    private static void pollCommitAsync(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCnt = 0;
        try {
            while(true) {
                // poll 메서드 수행은 Poll Thread가 수행 Main Thread X
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info("############ loopCnt: {}, ConsumerRecord count:{}", loopCnt++, records.count());
                for(ConsumerRecord<String, String> record : records) {
                    logger.info("record key: {}, partition: {}, record offset: {}, record value: {} ", record.key(), record.partition(), record.offset(),  record.value());
                }
                kafkaConsumer.commitAsync((offsets, exception) -> {
                    if(exception != null) {
                        logger.error("offsets {} is not completed, error {}", offsets, exception.getMessage());
                    }
                });
            }
        } catch (WakeupException e) {
            // consumer가 종료된 경우 발생하는 예외
            logger.error("wakeup exception has been called");
        } finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }
    }

}
