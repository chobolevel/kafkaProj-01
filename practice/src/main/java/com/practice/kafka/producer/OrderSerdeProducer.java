package com.practice.kafka.producer;

import com.practice.kafka.model.OrderModel;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class OrderSerdeProducer {

    private static final Logger logger = LoggerFactory.getLogger(OrderSerdeProducer.class);

    private static final String filePath = "C:\\dev\\소스 코드\\kafka-core\\practice\\src\\main\\resources\\pizza_sample.txt";

    public static void main(String[] args) {
        
        // 기본적인 전송 단계 KafkaProducer 객체 생성 -> ProducerRecords 생성 -> send() 비동기 방식 전송

        String topicName = "order-serde-topic";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSerializer.class.getName());

        KafkaProducer<String, OrderModel> kafkaProducer = new KafkaProducer<>(props);

        sendFileMessage(kafkaProducer, topicName, filePath);

        kafkaProducer.close();

    }

    private static void sendFileMessage(KafkaProducer<String, OrderModel> kafkaProducer,
                                        String topicName,
                                        String filePath) {
        String line = "";
        final String delimiter = ",";
        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            while( (line = bufferedReader.readLine()) != null) {
                String[] tokens = line.split(delimiter);
                String key = tokens[0];
                StringBuffer value = new StringBuffer();

                OrderModel orderModel = new OrderModel
                        (tokens[1],
                        tokens[2],
                        tokens[3],
                        tokens[4],
                        tokens[5],
                        tokens[6],
                        LocalDateTime.parse(tokens[7].trim(), formatter));

                sendMessage(kafkaProducer, topicName, key, orderModel);

            }


        } catch (IOException e) {
            logger.error(e.getMessage());
        }

    }

    private static void sendMessage(KafkaProducer<String, OrderModel> kafkaProducer,
                                    String topicName,
                                    String key,
                                    OrderModel orderModel) {
        ProducerRecord<String, OrderModel> record = new ProducerRecord<>(topicName, key, orderModel);
        logger.info("key: {}, value: {}", key, orderModel);
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
