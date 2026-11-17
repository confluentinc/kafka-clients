# C++ binding: accepted contract changes

A public item of the C++ binding whose documentation carries a `WARNING:` line (CLAUDE.md
§12.2) must have an entry here, in the format of the root `.claude/rules/documentation.md` §6.
An entry with an empty `Accepted` is proposed; only the user fills it. The lint fails on a
`WARNING` without an accepted entry, and on an accepted entry whose item or `WARNING` is gone.

The title is the C++ qualified name followed by the Java overload's parameter list in
`design/current/java-deprecated.txt`'s form (CLAUDE.md §12.2), so each overload has its own: a
type's title is its name alone. The four sub-items are required, in this order, and no others are
allowed. Titles are unique. `confluent-kafka-doc` checks all of it.

Format:

```markdown
1. `confluent::kafka::producer::ProducerRecord::ProducerRecord(String,Integer,Long,K,V,Iterable)`
   - Java member: `org.apache.kafka.clients.producer.ProducerRecord#ProducerRecord(String,Integer,Long,K,V,Iterable)`
   - Contract change: <what differs from the Java contract>
   - Why: <rationale>
   - Accepted: <empty = proposed; the user writes `YYYY-MM-DD Name`>
```

## Entries

(none yet)
