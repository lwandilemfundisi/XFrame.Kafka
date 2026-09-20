using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using XFrame.Common;
using XFrame.Kafka.Consumer.Connections;
using XFrame.Kafka.Resiliencies;
using XFrame.Resilience;

namespace XFrame.Kafka.Consumer
{
    public sealed class KafkaConsumer : IKafkaConsumer
    {
        private readonly ILogger<KafkaConsumer> _logger;
        private readonly IKafkaConsumerConnectionFactory _connectionFactory;
        private readonly ITransientFaultHandler<IKafkaResilientStrategy> _transientFaultHandler;

        public KafkaConsumer(
            ILogger<KafkaConsumer> logger, 
            IKafkaConsumerConnectionFactory _connectionFactory, 
            ITransientFaultHandler<IKafkaResilientStrategy> transientFaultHandler)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _connectionFactory = _connectionFactory ?? throw new ArgumentNullException(nameof(_connectionFactory));
            _transientFaultHandler = transientFaultHandler ?? throw new ArgumentNullException(nameof(_transientFaultHandler));
        }
            
        public async Task<ConsumeResult<string, string>> ConsumeAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            ConsumeResult<string, string> result = null;

            cancellationToken.ThrowIfCancellationRequested();

            var consumerConnection = _connectionFactory.CreateConsumerConnection<string, string>();

            try
            {
                await _transientFaultHandler.TryAsync(
                    action: async (c) => result = await consumerConnection.ConsumeAsync(timeout, c),
                    label: Label.Named("kafka-consumer"),
                    cancellationToken: cancellationToken);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch(Exception ex)
            {
                _logger.LogError(ex, "Failed to consume message from Kafka after executing all configured retries.");
            }

            return result;
        }
    }
}
