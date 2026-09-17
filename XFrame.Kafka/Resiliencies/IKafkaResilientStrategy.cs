namespace XFrame.Kafka.Resiliencies;

public interface IKafkaResilientStrategy
{
    KafkaRetryDecision CheckRetry(Exception exception, int currentRetryCount);
}

public readonly record struct KafkaRetryDecision(bool ShouldRetry, TimeSpan Delay)
{
    public static KafkaRetryDecision No => new(false, TimeSpan.Zero);
}
