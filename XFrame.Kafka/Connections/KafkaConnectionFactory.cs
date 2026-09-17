using Confluent.Kafka;
using XFrame.Kafka.Configurations;

namespace XFrame.Kafka.Connections;

public sealed class KafkaConnectionFactory(IKafkaConfiguration configuration) : IKafkaConnectionFactory
{
    public IKafkaConnection CreateConnection()
    {
        var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = configuration.BootstrapServers,
            ClientId = configuration.ClientId,
            Acks = configuration.Persistent ? Acks.All : Acks.None,
            EnableIdempotence = configuration.Persistent
        }).Build();
        return new KafkaConnection(producer);
    }
}
