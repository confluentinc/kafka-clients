package org.apache.kafka.common;
import org.apache.kafka.common.annotation.InterfaceAudience;
@InterfaceAudience.Public
public class Widget {
    public static final String KIND = "widget";
    public static final Widget EMPTY = new Widget("");
    public RuntimeException failure = null;
    public Widget(String name) {}
    public String getName() { return null; }
    public int count() { return 0; }
    public static Widget parse(String text) { return null; }
    public void unsupported(int x, int y) {}
}
