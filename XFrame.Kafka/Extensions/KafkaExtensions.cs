using Microsoft.Extensions.DependencyInjection;
using XFrame.Kafka.Configurations;
using XFrame.Kafka.Messages;
using XFrame.Kafka.Resiliencies;

namespace XFrame.Kafka.Extensions;

public static class KafkaExtensions
{
    public static IServiceCollection AddKafka(this IServiceCollection services, Func<IKafkaConfiguration> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        services.AddSingleton(_ => configure());
        services.AddSingleton<IKafkaMessageFactory, KafkaMessageFactory>();
        services.AddSingleton<IKafkaResilientStrategy, KafkaResilientStrategy>();
        return services;
    }
}
