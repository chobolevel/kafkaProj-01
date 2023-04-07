package com.example.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    private static final Logger logger = LoggerFactory.getLogger(CustomPartitioner.class);

    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();

    private String specialKeyName;

    // producer config 설정했던 부분을 여기서 조회할 수 있음
    @Override
    public void configure(Map<String, ?> configs) {
        specialKeyName = String.valueOf(configs.get("custom.specialKey"));
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 파티션 개수를 가져옴
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int numPartitions = partitionInfos.size();
        int numSpecialPartitions = (int) (numPartitions * 0.5);
        int partitionIndex = 0;

        if(keyBytes == null) {
            return stickyPartitionCache.partition(topic, cluster);
            // 또는 그냥 예외처리를 하는 선택도 있음
//            throw new InvalidRecordException("key value should not be null");
        }

        if(((String)key).equals(specialKeyName)) {
            // 0, 1 파티션으로 가도록 설정
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % numSpecialPartitions;
        } else {
            // 2, 3, 4 파티션으로 가도록 설정
            partitionIndex = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - numSpecialPartitions) + numSpecialPartitions;
        }

        logger.info("key: {} is send to partition: {}", key.toString(), partitionIndex);

        return partitionIndex;
    }

    @Override
    public void close() {

    }


}
