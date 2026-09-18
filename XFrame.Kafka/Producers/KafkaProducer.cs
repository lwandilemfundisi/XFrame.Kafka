using Confluent.Kafka;
using System.Text;
using XFrame.Kafka.Connections;
using XFrame.Kafka.Messages;

namespace XFrame.Kafka.Producers
{
    public class KafkaProducer : IKafkaProducer
    {
        private readonly IKafkaProducerConnectionFactory _kafkaProducerConnectionFactory;

        public KafkaProducer(IKafkaProducerConnectionFactory kafkaProducerConnectionFactory) 
        {
            _kafkaProducerConnectionFactory = kafkaProducerConnectionFactory;
        }

        public async Task<DeliveryResult<string, string>> PublishAsync(
        KafkaMessage message,
        CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(message);

            var headers = new Headers();
            headers.Add("xframe-message-id", Encoding.UTF8.GetBytes(message.MessageId.Value));

            foreach (var header in message.Headers)
                headers.Add(header.Key, Encoding.UTF8.GetBytes(header.Value));

            var producerConnection = _kafkaProducerConnectionFactory.CreateProducerConnection<string, string>();

            var result = await producerConnection.ProduceAsync(
            message.Topic,
            new Message<string, string> { Key = message.Key.Value, Value = message.Message, Headers = headers },
            cancellationToken).ConfigureAwait(false);

            return result;
        }
    }
}
