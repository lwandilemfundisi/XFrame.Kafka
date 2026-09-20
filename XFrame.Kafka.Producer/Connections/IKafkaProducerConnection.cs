using Confluent.Kafka;

namespace XFrame.Kafka.Producer.Connections
{
    public interface IKafkaProducerConnection<TKey, TValue>
    {
        Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        Topic topic,
        Message<TKey, TValue> message,
        CancellationToken cancellationToken);
    }
}
