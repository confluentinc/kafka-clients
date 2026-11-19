package org.apache.kafka.common;
import org.apache.kafka.common.annotation.InterfaceAudience;
@InterfaceAudience.Public
public interface Producer<K, V> { void send(K key); }
