using Confluent.Kafka;

namespace XFrame.Kafka.Connections;

public interface IKafkaConnection : IDisposable
{
    Task<DeliveryResult<string, string>> ProduceAsync(
        Topic topic, 
        Message<string, string> message, 
        CancellationToken cancellationToken);
}
