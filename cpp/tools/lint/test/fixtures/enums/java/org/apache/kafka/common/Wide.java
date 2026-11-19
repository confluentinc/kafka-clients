package org.apache.kafka.common;
import org.apache.kafka.common.annotation.InterfaceAudience;
@InterfaceAudience.Public
public enum Wide { A(1); private final int id; Wide(int id) { this.id = id; } }
