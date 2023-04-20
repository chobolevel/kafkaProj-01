package com.practice.kafka.producer;

import com.practice.kafka.event.EventHandler;
import com.practice.kafka.event.FileEventHandler;
import com.practice.kafka.event.FileEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.util.Properties;

public class FileAppendProducer {

    private static final File file = new File("D:\\dev\\소스 코드\\kafka-core\\kafkaProj-01\\practice\\src\\main\\resources\\pizza_append.txt");

    public static void main(String[] args) {

        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        boolean sync = false;

        EventHandler eventHandler = new FileEventHandler(kafkaProducer, topicName, sync);
        FileEventSource fileEventSource = new FileEventSource(1000, file, eventHandler);
        // runnable 구현체를 통해 Thread로 만들 수 있음
        Thread fileEventSourceThread = new Thread(fileEventSource);
        // start메서드를 실행하면 Runnable 구현체의 run()메서드 실행
        fileEventSourceThread.start();

        try {
            // FileEventSourceThread가 종료될 때까지 기다리는 코드
            fileEventSourceThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            kafkaProducer.close();
        }
    }

}
