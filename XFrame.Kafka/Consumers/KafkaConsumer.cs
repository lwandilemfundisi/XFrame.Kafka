using System.Text;
using Confluent.Kafka;
using XFrame.Kafka.Configurations;

namespace XFrame.Kafka.Consumers;

public sealed class KafkaConsumer : IKafkaConsumer
{
    private readonly IConsumer<string, string> _consumer;
    public KafkaConsumer(IKafkaConfiguration configuration) => 
        _consumer = new ConsumerBuilder<string, string>(
            new ConsumerConfig 
            { 
                BootstrapServers = configuration.BootstrapServers, 
                GroupId = configuration.ConsumerGroupId, 
                ClientId = configuration.ClientId, 
                AutoOffsetReset = AutoOffsetReset.Earliest, 
                EnableAutoCommit = false 
            })
        .Build();

    public void Subscribe(params string[] topics) => _consumer.Subscribe(topics);

    public KafkaConsumedMessage? Consume(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var result = _consumer.Consume(timeout);

        if (result is null || result.IsPartitionEOF) 
            return null;

        var headers = result
            .Message
            .Headers?
            .ToDictionary(h => h.Key, h => h.GetValueBytes() is { } bytes ? Encoding.UTF8.GetString(bytes) : string.Empty) ?? [];
        
        var id = headers.TryGetValue("xframe-message-id", out var value) ? value : Guid.NewGuid().ToString("N");

        var message = new KafkaConsumedMessage(
            new Topic(result.Topic), 
            new MessageKey(string.IsNullOrWhiteSpace(result.Message.Key) ? Guid.NewGuid().ToString("N") : result.Message.Key), 
            new MessageId(id), 
            result.Message.Value ?? string.Empty, 
            headers, 
            result.Partition.Value, 
            result.Offset.Value);

        _consumer.Commit(result);
        return message;
    }

    public void Dispose() 
    { 
        _consumer.Close(); 
        _consumer.Dispose(); 
    }
}
