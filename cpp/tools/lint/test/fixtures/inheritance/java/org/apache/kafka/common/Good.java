package org.apache.kafka.common;
import org.apache.kafka.common.annotation.InterfaceAudience;
@InterfaceAudience.Public
public class Good implements Producer<Integer, Integer> { public void send(Integer key) {} }
