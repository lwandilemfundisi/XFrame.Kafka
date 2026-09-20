using Confluent.Kafka;

namespace XFrame.Kafka.Consumer.Connections
{
    public class KafkaConsumerConnection<TKey, TValue> : IKafkaConsumerConnection<TKey, TValue>, IDisposable
    {
        private readonly IConsumer<TKey, TValue> _consumer;

        public KafkaConsumerConnection(IConsumer<TKey, TValue> consumer)
        {
            _consumer = consumer;
        }

        public Task<ConsumeResult<TKey, TValue>> ConsumeAsync(
            TimeSpan timeout, 
            CancellationToken cancellationToken = default)
        {
            var result = _consumer.Consume(timeout);
            _consumer.Commit();

            return Task.FromResult(result);
        }

        public void Dispose()
        {
            _consumer.Close();
            _consumer.Dispose();
        }
    }
}
