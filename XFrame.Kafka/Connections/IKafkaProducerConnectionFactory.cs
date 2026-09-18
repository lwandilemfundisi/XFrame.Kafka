using Confluent.Kafka;

namespace XFrame.Kafka.Connections
{
    public interface IKafkaProducerConnectionFactory
    {
        IKafkaProducerConnection<TKey, TValue> CreateProducerConnection<TKey, TValue>();
    }
}
