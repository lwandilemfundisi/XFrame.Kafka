namespace XFrame.Kafka.Producer.Connections
{
    public interface IKafkaProducerConnectionFactory
    {
        IKafkaProducerConnection<TKey, TValue> CreateProducerConnection<TKey, TValue>();
    }
}
