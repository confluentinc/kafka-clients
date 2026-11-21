package org.apache.kafka.common;
import org.apache.kafka.common.annotation.InterfaceAudience;
@InterfaceAudience.Public
public enum Ordered { A(0), B(1); private final int id; Ordered(int id) { this.id = id; } }
