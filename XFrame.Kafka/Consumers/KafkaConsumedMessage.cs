namespace XFrame.Kafka.Consumers;

public sealed record KafkaConsumedMessage(Topic Topic, MessageKey Key, MessageId MessageId, string Message, IReadOnlyDictionary<string, string> Headers, int Partition, long Offset);
