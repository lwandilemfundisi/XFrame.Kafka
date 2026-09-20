using Microsoft.Extensions.DependencyInjection;
using XFrame.Kafka.Producer.Connections;

namespace XFrame.Kafka.Producer.Extensions
{
    public static class KafkaProducerExtensions
    {
        public static IServiceCollection AddProducer(this IServiceCollection services)
        {
            services.AddSingleton<IKafkaProducerConnectionFactory, KafkaProducerConnectionFactory>();
            services.AddSingleton(provider => provider.GetRequiredService<IKafkaProducerConnectionFactory>().CreateProducerConnection<string, string>());
            services.AddSingleton<IKafkaProducer, KafkaProducer>();
            return services;
        }
    }
}
