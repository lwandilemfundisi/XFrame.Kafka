using Microsoft.Extensions.DependencyInjection;
using XFrame.Kafka.Consumer.Connections;

namespace XFrame.Kafka.Consumer.Extensions
{
    public static class KafkaConsumerExtensions
    {
        public static IServiceCollection AddConsumer(this IServiceCollection services)
        {
            services.AddSingleton<IKafkaConsumerConnectionFactory, KafkaConsumerConnectionFactory>();
            services.AddSingleton(provider => provider.GetRequiredService<IKafkaConsumerConnectionFactory>().CreateConsumerConnection<string, string>());
            services.AddSingleton<IKafkaConsumer, KafkaConsumer>();
            return services;
        }
    }
}
