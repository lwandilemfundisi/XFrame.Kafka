using Microsoft.Extensions.DependencyInjection;
using XFrame.Kafka.Configurations;
using XFrame.Kafka.Connections;
using XFrame.Kafka.Consumers;
using XFrame.Kafka.Messages;
using XFrame.Kafka.Publishers;
using XFrame.Kafka.Resiliencies;

namespace XFrame.Kafka.Extensions;

public static class KafkaExtensions
{
    public static IServiceCollection AddKafka(this IServiceCollection services, Func<IKafkaConfiguration> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        services.AddSingleton(_ => configure());
        services.AddSingleton<IKafkaConnectionFactory, KafkaConnectionFactory>();
        services.AddSingleton<IKafkaConnection>(provider => provider.GetRequiredService<IKafkaConnectionFactory>().CreateConnection());
        services.AddSingleton<IKafkaMessageFactory, KafkaMessageFactory>();
        services.AddSingleton<IKafkaPublisher, KafkaPublisher>();
        services.AddSingleton<IKafkaConsumer, KafkaConsumer>();
        services.AddSingleton<IKafkaResilientStrategy, KafkaResilientStrategy>();
        return services;
    }
}
