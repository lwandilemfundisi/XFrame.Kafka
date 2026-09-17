namespace XFrame.Kafka.Consumers;

public interface IKafkaConsumer : IDisposable
{
    void Subscribe(params string[] topics);
    KafkaConsumedMessage? Consume(TimeSpan timeout, CancellationToken cancellationToken = default);
}
