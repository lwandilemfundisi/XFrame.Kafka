using Confluent.Kafka;

namespace XFrame.Kafka.Connections
{
    public interface IKafkaProducerConnection<TKey, TValue>
    {
        Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        Topic topic,
        Message<TKey, TValue> message,
        CancellationToken cancellationToken);
    }
}
