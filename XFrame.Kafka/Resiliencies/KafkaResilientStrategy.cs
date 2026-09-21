using XFrame.Resilience;

namespace XFrame.Kafka.Resiliencies;

public class KafkaResilientStrategy : IKafkaResilientStrategy
{
    private const int MaximumRetryCount = 3;

    private static readonly ISet<Type> TransientExceptions = new HashSet<Type>
    {
        typeof(System.IO.IOException),
        typeof(System.Net.Sockets.SocketException),
        typeof(TimeoutException)
    };

    private static readonly ISet<Confluent.Kafka.ErrorCode> TransientKafkaErrorCodes =
        new HashSet<Confluent.Kafka.ErrorCode>
        {
            Confluent.Kafka.ErrorCode.LeaderNotAvailable,
            Confluent.Kafka.ErrorCode.NotLeaderForPartition,
            Confluent.Kafka.ErrorCode.RequestTimedOut,
            Confluent.Kafka.ErrorCode.BrokerNotAvailable,
            Confluent.Kafka.ErrorCode.ReplicaNotAvailable,
            Confluent.Kafka.ErrorCode.NetworkException,
            Confluent.Kafka.ErrorCode.GroupLoadInProgress,
            Confluent.Kafka.ErrorCode.GroupCoordinatorNotAvailable,
            Confluent.Kafka.ErrorCode.NotCoordinatorForGroup,
            Confluent.Kafka.ErrorCode.RebalanceInProgress,
            Confluent.Kafka.ErrorCode.Local_Transport,
            Confluent.Kafka.ErrorCode.Local_Resolve,
            Confluent.Kafka.ErrorCode.Local_MsgTimedOut,
            Confluent.Kafka.ErrorCode.Local_AllBrokersDown,
            Confluent.Kafka.ErrorCode.Local_TimedOut,
            Confluent.Kafka.ErrorCode.Local_QueueFull,
            Confluent.Kafka.ErrorCode.Local_TimedOutQueue,
            Confluent.Kafka.ErrorCode.Local_Retry
        };

    public Repeat CheckRetry(Exception exception, TimeSpan totalExecutionTime, int currentRetryCount)
    {
        if (currentRetryCount >= MaximumRetryCount || !IsTransient(exception))
            return Repeat.No;

        var retryDelay = TimeSpan.FromMilliseconds(200 * Math.Pow(2, currentRetryCount));
        return Repeat.YesAfter(retryDelay);
    }

    private static bool IsTransient(Exception exception) =>
        exception is Confluent.Kafka.KafkaException kafkaException
            ? TransientKafkaErrorCodes.Contains(kafkaException.Error.Code)
            : TransientExceptions.Contains(exception.GetType());
}
