namespace XFrame.Kafka.Configurations;

public interface IKafkaConfiguration
{
    string BootstrapServers { get; }
    string ClientId { get; }
    string ConsumerGroupId { get; }
    bool Persistent { get; }
    string Topic { get; }
}
