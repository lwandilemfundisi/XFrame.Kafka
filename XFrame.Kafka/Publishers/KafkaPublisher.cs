using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using XFrame.Kafka.Connections;
using XFrame.Kafka.Messages;

namespace XFrame.Kafka.Publishers;

public sealed class KafkaPublisher(IKafkaConnection connection, ILogger<KafkaPublisher> log) : IKafkaPublisher
{
    public async Task<DeliveryResult<string, string>> PublishAsync(KafkaMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        var headers = new Headers();
        headers.Add("xframe-message-id", Encoding.UTF8.GetBytes(message.MessageId.Value));
        foreach (var header in message.Headers) headers.Add(header.Key, Encoding.UTF8.GetBytes(header.Value));
        var result = await connection.ProduceAsync(message.Topic, new Message<string, string> { Key = message.Key.Value, Value = message.Message, Headers = headers }, cancellationToken).ConfigureAwait(false);
        log.LogInformation("Published Kafka message {MessageId} to {Topic} partition {Partition} offset {Offset}", message.MessageId, message.Topic, result.Partition, result.Offset);
        return result;
    }
}
