package org.apache.kafka.common;
import org.apache.kafka.common.annotation.InterfaceAudience;
@InterfaceAudience.Public
public enum Narrow { X((byte) 32); private final byte id; Narrow(byte id) { this.id = id; } }
