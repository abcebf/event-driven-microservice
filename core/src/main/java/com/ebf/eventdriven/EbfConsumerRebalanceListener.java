package com.ebf.eventdriven;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;

/**
 * Created by hhuang on 8/4/16.
 */
public class EbfConsumerRebalanceListener implements ConsumerRebalanceListener {
  private KafkaConsumer<String, String> consumer;
  private Map<TopicPartition, OffsetAndMetadata> currentOffsets;

  public EbfConsumerRebalanceListener(
      KafkaConsumer<String, String> consumer,
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    this.consumer = consumer;
    this.currentOffsets = currentOffsets;
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    //called before the rebalancing starts and after the consumer stopped consuming messages.
    // This is where you want to commit offsets,
    // so whoever gets this partition next will know where to start.
    consumer.commitSync(currentOffsets);
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    //called after partitions has been re-assigned to the broker,
    // but before the consumer started consuming messages
  }
}
