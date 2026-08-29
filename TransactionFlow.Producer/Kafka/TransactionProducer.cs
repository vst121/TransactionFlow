using Confluent.Kafka;
using Microsoft.Extensions.Options;
using System.Text.Json;
using TransactionFlow.Contracts;

namespace TransactionFlow.Producer.Kafka;

public sealed class TransactionProducer : ITransactionProducer, IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly IProducer<string, string> _producer;
    private readonly KafkaOptions _options;

    public TransactionProducer(
        IOptions<KafkaOptions> options)
    {
        _options = options.Value;

        var config = new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,

            // Reliable publishing
            EnableIdempotence = true,
            Acks = Acks.All,

            // We want retries to be safe
            MessageSendMaxRetries = 10
        };

        _producer =
            new ProducerBuilder<string, string>(config)
                .Build();
    }

    public async Task<DeliveryResult<string, string>> PublishAsync(
        TransactionMessage transaction,
        CancellationToken cancellationToken)
    {
        var json =
            JsonSerializer.Serialize(
                transaction,
                JsonOptions);

        var message = new Message<string, string>
        {
            // Important:
            // same merchant → same Kafka partition
            Key = transaction.MerchantId,

            Value = json
        };

        return await _producer.ProduceAsync(
            _options.Topic,
            message,
            cancellationToken);
    }

    public void Dispose()
    {
        _producer.Flush(
            TimeSpan.FromSeconds(5));

        _producer.Dispose();
    }
}