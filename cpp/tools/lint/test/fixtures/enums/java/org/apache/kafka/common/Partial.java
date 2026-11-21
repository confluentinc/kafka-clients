package org.apache.kafka.common;
import org.apache.kafka.common.annotation.InterfaceAudience;
@InterfaceAudience.Public
public enum Partial { A(0), B(1); private final int id; Partial(int id) { this.id = id; } }
