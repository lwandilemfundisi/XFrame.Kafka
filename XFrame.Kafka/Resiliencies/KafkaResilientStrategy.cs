using Confluent.Kafka;

namespace XFrame.Kafka.Resiliencies;

public sealed class KafkaResilientStrategy : IKafkaResilientStrategy
{
    public KafkaRetryDecision CheckRetry(Exception exception, int currentRetryCount)
    {
        var transient = exception is KafkaException or TimeoutException or IOException or System.Net.Sockets.SocketException;
        return transient && currentRetryCount <= 3
            ? new KafkaRetryDecision(true, TimeSpan.FromMilliseconds(100 * Math.Pow(2, Math.Max(1, currentRetryCount))))
            : KafkaRetryDecision.No;
    }
}
