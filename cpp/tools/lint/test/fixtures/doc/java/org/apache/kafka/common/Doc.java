package org.apache.kafka.common;

import org.apache.kafka.common.annotation.InterfaceAudience;

/** A documented class. */
@InterfaceAudience.Public
public class Doc {
    /** Creates it.
     * @param name the name */
    public Doc(String name) {}
    /** Returns the name.
     * @return the name */
    public String name() { return null; }
    /** Sets the size.
     * @param size the size
     * @throws IllegalArgumentException if the size is negative */
    public void size(int size) {}
    /** Short. */
    public void shortOne() {}
    public void undocumented() {}
    /** Warns. Says what the method does with enough words that a short note fits beside it. */
    public void warns() {}
    /** Warns too. Says what the method does with enough words that a short note fits beside it. */
    public void pending() {}
    /** Accepted. Says what the method does with enough words that a short note fits beside it. */
    public void accepted() {}
    /** Leftover. Says what the method does with enough words that a short note fits beside it. */
    public void leftover() {}
    /** Twice the notes. Says what the method does with enough words that a short note fits beside it. */
    public void twice() {}
    /** Stray. */
    public void stray() {}
    /** Fine.
     * @param size the size
     * @return the size */
    public int fine(int size) { return size; }
}
