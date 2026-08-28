using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using TransactionFlow.Contracts;

namespace TransactionFlow.Worker.Kafka;

public sealed class TransactionConsumer(
    IOptions<KafkaOptions> options,
    ILogger<TransactionConsumer> logger)
    : BackgroundService
{
    private readonly KafkaOptions _options = options.Value;

    // Cached JsonSerializerOptions to avoid per-message allocations (CA1869).
    private static readonly JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true
    };

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,

            GroupId = _options.GroupId,

            AutoOffsetReset = AutoOffsetReset.Earliest,

            EnableAutoCommit = false,

            EnableAutoOffsetStore = false,

            AllowAutoCreateTopics = false
        };

        using var consumer =
            new ConsumerBuilder<string, string>(config)
                .SetErrorHandler((_, error) =>
                {
                    logger.LogError(
                        "Kafka error: {Reason}",
                        error.Reason);
                })
                .Build();

        consumer.Subscribe(_options.Topic);

        logger.LogInformation(
            "Kafka consumer started. Topic: {Topic}, GroupId: {GroupId}",
            _options.Topic,
            _options.GroupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result =
                        consumer.Consume(stoppingToken);

                    try
                    {
                        var message =
                            JsonSerializer.Deserialize<TransactionMessage>(
                                result.Message.Value,
                                _jsonOptions);

                        if (message is null)
                        {
                            logger.LogWarning(
                                "Received null transaction. " +
                                "Partition={Partition}, Offset={Offset}",
                                result.Partition,
                                result.Offset);

                            continue;
                        }

                        logger.LogInformation(
                            """
        Transaction received:
        Id={TransactionId}
        Merchant={MerchantId}
        Amount={Amount}
        Currency={Currency}
        Status={Status}
        Partition={Partition}
        Offset={Offset}
        """,
                            message.TransactionId,
                            message.MerchantId,
                            message.Amount,
                            message.Currency,
                            message.Status,
                            result.Partition,
                            result.Offset);
                    }
                    catch (JsonException ex)
                    {
                        logger.LogError(
                            ex,
                            "Invalid JSON message. Partition={Partition}, Offset={Offset}, Value={Value}",
                            result.Partition,
                            result.Offset,
                            result.Message.Value);

                        continue;
                    }

                    // IMPORTANT:
                    // We do NOT commit the offset yet.
                }
                catch (ConsumeException ex)
                {
                    logger.LogError(
                        ex,
                        "Kafka consume failed: {Reason}",
                        ex.Error.Reason);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during graceful shutdown.
        }
        finally
        {
            consumer.Close();
        }
    }
}