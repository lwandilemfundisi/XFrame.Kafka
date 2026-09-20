using Confluent.Kafka;

namespace XFrame.Kafka.Producer.Connections
{
    public class KafkaProducerConnection<TKey, TValue> : IKafkaProducerConnection<TKey, TValue>
    {
        private readonly IProducer<TKey, TValue> _producer;

        public KafkaProducerConnection(IProducer<TKey, TValue> producer)
        {
            _producer = producer;
        }

        public Task<DeliveryResult<TKey, TValue>> ProduceAsync(
            Topic topic,
            Message<TKey, TValue> message,
            CancellationToken cancellationToken)
        {
            return _producer.ProduceAsync(topic.Value, message, cancellationToken);
        }
    }
}
