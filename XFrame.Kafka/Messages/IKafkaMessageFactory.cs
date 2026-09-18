namespace XFrame.Kafka.Messages;

public interface IKafkaMessageFactory
{
    KafkaMessage CreateMessage<TPayload>(
        TPayload payload, 
        IReadOnlyDictionary<string, string>? headers = null, 
        string? key = null, 
        string? topic = null) where TPayload : class;
}
