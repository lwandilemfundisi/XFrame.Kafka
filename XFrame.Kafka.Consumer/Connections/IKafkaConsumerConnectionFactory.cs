namespace XFrame.Kafka.Consumer.Connections
{
    public interface IKafkaConsumerConnectionFactory
    {
        IKafkaConsumerConnection<TKey, TValue> CreateConsumerConnection<TKey, TValue>();
    }
}
