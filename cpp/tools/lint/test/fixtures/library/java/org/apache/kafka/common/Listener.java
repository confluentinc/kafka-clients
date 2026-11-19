package org.apache.kafka.common;

import org.apache.kafka.common.annotation.InterfaceAudience;

@InterfaceAudience.Public
public interface Listener { void onEvent(); }
