# Rebalance Protocols

Kafka supports different consumer group rebalancing protocols:

- Kafka 0.9.0 supports https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal. This is
  referred to as the "new consumer", even though it's 10 years old at this point.
  - **This is the protocol currently supported by Kafine.**
- KIP-429 introduces the "incremental rebalance protocol"; the one above is now referred to as the "eager rebalance
  protocol".
  - See these links for more detail:
    - <https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol>
    - <https://www.confluent.io/blog/incremental-cooperative-rebalancing-in-kafka/>
    - <https://www.confluent.io/kafka-summit-san-francisco-2019/why-stop-the-world-when-you-can-change-it-design-and-implementation-of-incremental-cooperative-rebalancing/>
    - <https://cwiki.apache.org/confluence/display/KAFKA/KIP-441%3A+Smooth+Scaling+Out+for+Kafka+Streams>
  - This protocol is about 5 years old at this point.
  - **This is not supported by Kafine.**
- KIP-848 (and Kafka 4.x) introduces the "next generation of the consumer rebalance protocol".
  - **This is not supported by Kafine.**
