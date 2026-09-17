namespace XFrame.Kafka.Connections;

public interface IKafkaConnectionFactory
{
    IKafkaConnection CreateConnection();
}
