using Confluent.Kafka;
using XFrame.Kafka.Messages;

namespace XFrame.Kafka.Producer
{
    public interface IKafkaProducer
    {
        Task<DeliveryResult<string, string>> PublishAsync(
        KafkaMessage message,
        CancellationToken cancellationToken = default);
    }
}
