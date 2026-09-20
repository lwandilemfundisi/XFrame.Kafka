using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Text;
using XFrame.Common;
using XFrame.Kafka.Messages;
using XFrame.Kafka.Producer.Connections;
using XFrame.Kafka.Resiliencies;
using XFrame.Resilience;

namespace XFrame.Kafka.Producer
{
    public class KafkaProducer : IKafkaProducer
    {
        private readonly ILogger<KafkaProducer> _logger;
        private readonly IKafkaProducerConnectionFactory _kafkaProducerConnectionFactory;
        private readonly ITransientFaultHandler<IKafkaResilientStrategy> _transientFaultHandler;

        public KafkaProducer(
            ILogger<KafkaProducer> logger, 
            IKafkaProducerConnectionFactory kafkaProducerConnectionFactory, 
            ITransientFaultHandler<IKafkaResilientStrategy> transientFaultHandler)
        {
            _logger = logger;
            _kafkaProducerConnectionFactory = kafkaProducerConnectionFactory;
            _transientFaultHandler = transientFaultHandler;
        }

        public async Task<DeliveryResult<string, string>> PublishAsync(
        KafkaMessage message,
        CancellationToken cancellationToken = default)
        {
            DeliveryResult<string, string> result = null;

            ArgumentNullException.ThrowIfNull(message);

            var producerConnection = _kafkaProducerConnectionFactory.CreateProducerConnection<string, string>();

            try
            {
                await _transientFaultHandler.TryAsync(
                    action: async (c) =>
                    {
                        var kmessage = CreateMessage(message);

                        result = await producerConnection.ProduceAsync(
                            kmessage.Key,
                            kmessage.Value,
                            c).ConfigureAwait(false);
                    },
                    label: Label.Named("kafka-producer"),
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            catch(OperationCanceledException)
            {
                throw;
            }

            return result;
        }

        private KeyValuePair<Topic, Message<string, string>> CreateMessage(KafkaMessage message)
        {
            var headers = new Headers
            {
                { "xframe-message-id", Encoding.UTF8.GetBytes(message.MessageId.Value) }
            };

            foreach (var header in message.Headers)
                headers.Add(header.Key, Encoding.UTF8.GetBytes(header.Value));

            var kafkaMessage = new Message<string, string>
            {
                Key = message.Key.Value,
                Value = message.Message,
                Headers = headers
            };
            return new KeyValuePair<Topic, Message<string, string>>(message.Topic, kafkaMessage);
        }
    }
}
