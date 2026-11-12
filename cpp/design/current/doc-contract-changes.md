# C++ binding: accepted contract changes

A public item of the C++ binding whose documentation carries a `WARNING:` line (CLAUDE.md
§12.2) must have an entry here, in the format of the root `.claude/rules/documentation.md` §6.
An entry with an empty `Accepted` is proposed; only the user fills it. The lint fails on a
`WARNING` without an accepted entry, and on an accepted entry whose item or `WARNING` is gone.

The title is the C++ qualified name (`confluent::kafka::...`). The four sub-items are required, in
this order, and no others are allowed. Titles are unique.

Format:

```markdown
1. `confluent::kafka::producer::ProducerRecord::ProducerRecord`
   - Java member: `org.apache.kafka.clients.producer.ProducerRecord#ProducerRecord(String,Integer,Long,Object,Object,Iterable)`
   - Contract change: <what differs from the Java contract>
   - Why: <rationale>
   - Accepted: <empty = proposed; the user writes `YYYY-MM-DD Name`>
```

## Entries

(none yet)
