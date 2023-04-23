package com.practice.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FileToDBConsumer<K extends Serializable, V extends Serializable> {
    public static final Logger logger = LoggerFactory.getLogger(FileToDBConsumer.class.getName());
    protected KafkaConsumer<K, V> kafkaConsumer;
    protected List<String> topics;

    private OrderDBHandler orderDBHandler;
    public FileToDBConsumer(Properties consumerProps, List<String> topics,
                            OrderDBHandler orderDBHandler) {
        this.kafkaConsumer = new KafkaConsumer<K, V>(consumerProps);
        this.topics = topics;
        this.orderDBHandler = orderDBHandler;
    }
    public void initConsumer() {
        this.kafkaConsumer.subscribe(this.topics);
        shutdownHookToRuntime(this.kafkaConsumer);
    }

    private void shutdownHookToRuntime(KafkaConsumer<K, V> kafkaConsumer) {
        //main thread
        Thread mainThread = Thread.currentThread();

        //main thread 종료시 별도의 thread로 KafkaConsumer wakeup()메소드를 호출하게 함.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info(" main program starts to exit by calling wakeup");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch(InterruptedException e) { e.printStackTrace();}
            }
        });

    }

    private void processRecord(ConsumerRecord<K, V> record) throws Exception {
        // 단건에 대해 DB insert를 위해 바인딩 하는 메서드를 호출하고 return된 OrderDTO로 insert 하는 메서드
        OrderDTO orderDTO = makeOrderDTO(record);
        this.orderDBHandler.insertOrder(orderDTO);
    }

    private OrderDTO makeOrderDTO(ConsumerRecord<K,V> record) throws Exception {
        // OrderDTO로 바인딩을 수행하는 메서드
        String messageValue = (String) record.value();
        logger.info("##### messageValue: {}", messageValue);
        String[] tokens = messageValue.split(",");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        OrderDTO orderDTO = new OrderDTO(tokens[0], tokens[1], tokens[2], tokens[3],
                tokens[4], tokens[5], LocalDateTime.parse(tokens[6].trim(), formatter));
        return orderDTO;
    }

    private void processRecords(ConsumerRecords<K, V> records) throws Exception {
        // 다량건에 대해 OrderDTO List를 받고, DB inserts를 수행하는 메서드
        List<OrderDTO> orders = makeOrders(records);
        this.orderDBHandler.insertOrders(orders);
    }

    private List<OrderDTO> makeOrders(ConsumerRecords<K,V> records) throws Exception {
        // OrderDTO로 바인딩해서 List를 만드는 메서드
        List<OrderDTO> orders = new ArrayList<>();
        for(ConsumerRecord<K, V> record : records) {
            OrderDTO orderDTO = this.makeOrderDTO(record);
            orders.add(orderDTO);
        }
        return orders;
    }


    public void pollConsumes(long durationMillis, String commitMode) {
        // 동기 type에 따라 sync,async 방식으로 메세지 전송
        if (commitMode.equals("sync")) {
            pollCommitSync(durationMillis);
        } else {
            pollCommitAsync(durationMillis);
        }
    }
    private void pollCommitAsync(long durationMillis) {
        try {
            while (true) {
                ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
                logger.info("consumerRecords count:" + consumerRecords.count());
                // 다량건으로 처리
                if(consumerRecords.count() > 0) {
                    // 여기서 예외 발생 시 해당 레코드에 대해서는 재처리하지 않게 되지만 while문은 계속 수행됨
                    try {
                        processRecords(consumerRecords);
                    } catch(Exception e) {
                        logger.error(e.getMessage());
                    }
                }
                // 단건을 for문을 이용해서 처리
//                if(consumerRecords.count() > 0) {
//                    for (ConsumerRecord<K, V> consumerRecord : consumerRecords) {
//                        processRecord(consumerRecord);
//                    }
//                }
                //commitAsync의 OffsetCommitCallback을 lambda 형식으로 변경
                this.kafkaConsumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        logger.error("offsets {} is not completed, error:{}", offsets, exception.getMessage());
                    }
                });
            }
        }catch(WakeupException e) {
            logger.error("wakeup exception has been called");
        }catch(Exception e) {
            logger.error(e.getMessage());
        }finally {
            logger.info("##### commit sync before closing");
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            close();
        }
    }

    protected void pollCommitSync(long durationMillis) {
        try {
            while (true) {
                ConsumerRecords<K, V> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(durationMillis));
                processRecords(consumerRecords);
                try {
                    if (consumerRecords.count() > 0) {
                        this.kafkaConsumer.commitSync();
                        logger.info("commit sync has been called");
                    }
                } catch (CommitFailedException e) {
                    logger.error(e.getMessage());
                }
            }
        }catch(WakeupException e) {
            logger.error("wakeup exception has been called");
        }catch(Exception e) {
            logger.error(e.getMessage());
        }finally {
            logger.info("##### commit sync before closing");
            kafkaConsumer.commitSync();
            logger.info("finally consumer is closing");
            close();
        }
    }
    protected void close() {
        this.kafkaConsumer.close();
        this.orderDBHandler.close();
    }

    public static void main(String[] args) {
        String topicName = "file-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "file-group");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        String url = "jdbc:mariadb://localhost:3306/kafka";
        String user = "root";
        String password = "1234";
        OrderDBHandler orderDBHandler = new OrderDBHandler(url, user, password);

        FileToDBConsumer<String, String> fileToDBConsumer = new
                FileToDBConsumer<>(props, List.of(topicName), orderDBHandler);
        fileToDBConsumer.initConsumer();
        String commitMode = "async";

        fileToDBConsumer.pollConsumes(1000, commitMode);
        // while(true)이기 때문에 도달하지 못하는 코드
        fileToDBConsumer.close();

    }

}