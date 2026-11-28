package org.apache.kafka.common;
import org.apache.kafka.common.annotation.InterfaceAudience;
@InterfaceAudience.Public
public enum Valued { A(1), B(2); private final int id; Valued(int id) { this.id = id; } }
