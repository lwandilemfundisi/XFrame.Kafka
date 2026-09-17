using Confluent.Kafka;

namespace XFrame.Kafka.Connections;

public sealed class KafkaConnection(IProducer<string, string> producer) : IKafkaConnection
{
    public Task<DeliveryResult<string, string>> ProduceAsync(Topic topic, Message<string, string> message, CancellationToken cancellationToken) => producer.ProduceAsync(topic.Value, message, cancellationToken);
    public void Dispose() => producer.Dispose();
}
