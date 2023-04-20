package com.practice.kafka.event;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class FileEventHandler implements EventHandler{

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topicName;
    private final boolean sync;
    private final Logger logger = LoggerFactory.getLogger(FileEventHandler.class);

    public FileEventHandler(KafkaProducer<String, String> kafkaProducer, String topicName, boolean sync) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
        this.sync = sync;
    }

    @Override
    public void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException {
        ProducerRecord<String, String> record = new ProducerRecord<>(this.topicName,
                messageEvent.getKey(),
                messageEvent.getValue());
        if(this.sync) {
            RecordMetadata metadata = kafkaProducer.send(record).get();
            logger.info("\n##### record metadata received #####\n" +
                    "partition: " + metadata.partition() + "\n" +
                    "offset: " + metadata.offset() + "\n" +
                    "timestamp: " + metadata.timestamp());
        } else {
            kafkaProducer.send(record, ((metadata, exception) -> {
                if(exception == null) {
                    logger.info("\n##### record metadata received #####\n" +
                            "partition: " + metadata.partition() + "\n" +
                            "offset: " + metadata.offset() + "\n" +
                            "timestamp: " + metadata.timestamp());
                } else {
                    logger.error("exception error from broker " + exception.getMessage());
                }
            }));
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // FileEventHandler test code
        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        boolean sync = true;

        FileEventHandler fileEventHandler = new FileEventHandler(kafkaProducer, topicName, sync);
        MessageEvent messageEvent = new MessageEvent("key00001", "this is test message");
        fileEventHandler.onMessage(messageEvent);

    }

}
