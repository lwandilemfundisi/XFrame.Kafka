using XFrame.Resilience;

namespace XFrame.Kafka.Resiliencies;

public sealed class KafkaResilientStrategy : IKafkaResilientStrategy
{
    private static readonly ISet<Type> TransientExceptions = new HashSet<Type>
        {
        };

    public Repeat CheckRetry(Exception exception, TimeSpan totalExecutionTime, int currentRetryCount)
    {
        return Repeat.No;
    }
}
