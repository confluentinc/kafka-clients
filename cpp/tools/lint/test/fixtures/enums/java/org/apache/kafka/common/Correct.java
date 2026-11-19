package org.apache.kafka.common;
import org.apache.kafka.common.annotation.InterfaceAudience;
@InterfaceAudience.Public
public enum Correct { A(1), B(2); private final int id; Correct(int id) { this.id = id; } }
