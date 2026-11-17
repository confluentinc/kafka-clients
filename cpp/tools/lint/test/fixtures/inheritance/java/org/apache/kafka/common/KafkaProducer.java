package org.apache.kafka.common;
import org.apache.kafka.common.annotation.InterfaceAudience;
@InterfaceAudience.Public
public class KafkaProducer<K, V> implements Producer<K, V> { public void send(K key) {} }
