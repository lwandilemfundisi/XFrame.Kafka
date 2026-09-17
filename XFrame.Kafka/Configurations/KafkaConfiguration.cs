namespace XFrame.Kafka.Configurations;

public sealed class KafkaConfiguration : IKafkaConfiguration
{
    private KafkaConfiguration(string bootstrapServers, string clientId, string consumerGroupId, bool persistent, string topic)
    {
        BootstrapServers = bootstrapServers;
        ClientId = clientId;
        ConsumerGroupId = consumerGroupId;
        Persistent = persistent;
        Topic = topic;
    }

    public string BootstrapServers { get; }
    public string ClientId { get; }
    public string ConsumerGroupId { get; }
    public bool Persistent { get; }
    public string Topic { get; }

    public static IKafkaConfiguration With(string bootstrapServers, string topic, string? consumerGroupId = null, string? clientId = null, bool persistent = true)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(bootstrapServers);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        return new KafkaConfiguration(bootstrapServers, clientId ?? Environment.MachineName, consumerGroupId ?? $"{topic}.consumer", persistent, topic);
    }

    public static IKafkaConfiguration With(Uri uri, string topic, string? consumerGroupId = null, string? clientId = null, bool persistent = true)
    {
        ArgumentNullException.ThrowIfNull(uri);
        return With(uri.Authority, topic, consumerGroupId, clientId, persistent);
    }
}
