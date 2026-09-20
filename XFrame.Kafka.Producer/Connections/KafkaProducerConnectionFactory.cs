using Confluent.Kafka;
using XFrame.Kafka.Configurations;

namespace XFrame.Kafka.Producer.Connections
{
    public class KafkaProducerConnectionFactory : IKafkaProducerConnectionFactory
    {
        private readonly IKafkaConfiguration _configuration;

        public KafkaProducerConnectionFactory(IKafkaConfiguration configuration)
        {
            _configuration = configuration;
        }

        public IKafkaProducerConnection<TKey, TValue> CreateProducerConnection<TKey, TValue>()
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = _configuration.BootstrapServers,
                ClientId = _configuration.ClientId,
                Acks = _configuration.Persistent ? Acks.All : Acks.None,
                EnableIdempotence = _configuration.Persistent
            };

            var producer = new ProducerBuilder<TKey, TValue>(producerConfig)
                .Build();

            return new KafkaProducerConnection<TKey, TValue>(producer);
        }
    }
}
