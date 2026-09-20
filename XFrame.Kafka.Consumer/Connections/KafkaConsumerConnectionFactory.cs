using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using XFrame.Kafka.Configurations;

namespace XFrame.Kafka.Consumer.Connections
{
    public class KafkaConsumerConnectionFactory : IKafkaConsumerConnectionFactory
    {
        private readonly IKafkaConfiguration _configuration;

        public KafkaConsumerConnectionFactory(IKafkaConfiguration configuration)
        {
            _configuration = configuration;
        }

        public IKafkaConsumerConnection<TKey, TValue> CreateConsumerConnection<TKey, TValue>()
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _configuration.BootstrapServers,
                GroupId = _configuration.ConsumerGroupId,
                ClientId = _configuration.ClientId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            var consumer = new ConsumerBuilder<TKey, TValue>(consumerConfig)
                .Build();

            return new KafkaConsumerConnection<TKey, TValue>(consumer);
        }
    }
}
