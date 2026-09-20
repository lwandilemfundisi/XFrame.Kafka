using Confluent.Kafka;

namespace XFrame.Kafka.Consumer.Connections
{
    public interface IKafkaConsumerConnection<TKey, TValue>
    {
        Task<ConsumeResult<TKey, TValue>> ConsumeAsync(
            TimeSpan timeout,
            CancellationToken cancellationToken = default);
    }
}
