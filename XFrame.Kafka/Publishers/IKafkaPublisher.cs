using Confluent.Kafka;
using XFrame.Kafka.Messages;

namespace XFrame.Kafka.Publishers;

public interface IKafkaPublisher
{
    Task<DeliveryResult<string, string>> PublishAsync(
        KafkaMessage message, 
        CancellationToken cancellationToken = default);
}
