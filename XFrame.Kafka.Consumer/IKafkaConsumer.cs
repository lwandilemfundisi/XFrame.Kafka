using Confluent.Kafka;

namespace XFrame.Kafka.Consumer
{
    public interface IKafkaConsumer
    {
        Task<ConsumeResult<string, string>> ConsumeAsync(
            TimeSpan timeout, 
            CancellationToken cancellationToken = default);
    }
}
