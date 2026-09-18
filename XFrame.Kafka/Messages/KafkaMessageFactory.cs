using System.Text.Json;
using Microsoft.Extensions.Logging;
using XFrame.Kafka.Configurations;

namespace XFrame.Kafka.Messages;

public sealed class KafkaMessageFactory(
    ILogger<KafkaMessageFactory> log, 
    IKafkaConfiguration configuration) : IKafkaMessageFactory
{
    public KafkaMessage CreateMessage<TPayload>(
        TPayload payload, 
        IReadOnlyDictionary<string, string>? headers = null, 
        string? key = null, 
        string? topic = null) where TPayload : class
    {
        ArgumentNullException.ThrowIfNull(payload);

        var message = new KafkaMessage(
            JsonSerializer.Serialize(payload), 
            headers ?? new Dictionary<string, string>(), 
            new Topic(topic ?? configuration.Topic), 
            new MessageKey(key ?? Guid.NewGuid().ToString("N")), 
            new MessageId(Guid.NewGuid().ToString("N")));

        log.LogInformation("Created Kafka message {Message}", message);
        return message;
    }
}
