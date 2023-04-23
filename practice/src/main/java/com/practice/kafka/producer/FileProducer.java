package com.practice.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class FileProducer {

    private static final Logger logger = LoggerFactory.getLogger(FileProducer.class);

    private static final String filePath = "C:\\dev\\강의별 소스코드\\kafka-core\\kafkaProj-01\\practice\\src\\main\\resources\\pizza_append.txt";

    public static void main(String[] args) {
        
        // 기본적인 전송 단계 KafkaProducer 객체 생성 -> ProducerRecords 생성 -> send() 비동기 방식 전송

        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        sendFileMessage(kafkaProducer, topicName, filePath);

        kafkaProducer.close();

    }

    private static void sendFileMessage(KafkaProducer<String, String> kafkaProducer,
                                        String topicName,
                                        String filePath) {
        String line = "";
        final String delimiter = ",";
        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while( (line = bufferedReader.readLine()) != null) {
                String[] tokens = line.split(delimiter);
                String key = tokens[0];
                StringBuffer value = new StringBuffer();

                for(int i = 1; i < tokens.length - 1; i++) {
                    value.append(tokens[i] + ",");
                }
                value.append(tokens[tokens.length - 1]);

                sendMessage(kafkaProducer, topicName, key, value.toString());

            }


        } catch (IOException e) {
            logger.error(e.getMessage());
        }

    }

    private static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                    String topicName,
                                    String key,
                                    String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
        logger.info("key: {}, value: {}", key, value);
        kafkaProducer.send(record, ((metadata, exception) -> {
            if(exception == null) {
                logger.info("\n##### record metadata received #####\n" +
                        "partition: " + metadata.partition() + "\n" +
                        "offset: " + metadata.offset() + "\n" +
                        "timestamp: " + metadata.timestamp());
            } else {
                logger.error("exception error from broker = {}", exception.getMessage());
            }
        }));
    }

}
