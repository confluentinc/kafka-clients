package org.apache.kafka.common;
import org.apache.kafka.common.annotation.InterfaceAudience;
@InterfaceAudience.Public
public class Gadget {
    public Gadget(String name, int size) {}
    public long total(int a, String label) { return 0; }
    public long length() { return 0; }
    public String describe() { return null; }
    public void rename(String name) {}
    public static Gadget create() { return null; }
    public String label(String key) { return null; }
    public String tag(String key) { return null; }
}
