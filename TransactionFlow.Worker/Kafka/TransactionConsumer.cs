using Confluent.Kafka;
using Microsoft.Extensions.Options;
using System.Text.Json;
using TransactionFlow.Application.Transactions;
using TransactionFlow.Contracts;

namespace TransactionFlow.Worker.Kafka;

public sealed class TransactionConsumer(
    IOptions<KafkaOptions> options,
    IServiceScopeFactory scopeFactory,
    ILogger<TransactionConsumer> logger)
    : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly KafkaOptions _options = options.Value;

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
            "Kafka consumer started. Topic={Topic}, GroupId={GroupId}",
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

                    TransactionMessage? message;

                    try
                    {
                        message =
                            JsonSerializer.Deserialize<TransactionMessage>(
                                result.Message.Value,
                                JsonOptions);
                    }
                    catch (JsonException ex)
                    {
                        logger.LogError(
                            ex,
                            "Invalid JSON. Partition={Partition}, Offset={Offset}",
                            result.Partition,
                            result.Offset);

                        // We intentionally don't commit here.
                        // DLQ/retry handling will be added later.
                        continue;
                    }

                    if (message is null)
                    {
                        logger.LogError(
                            "Message deserialized to null. Partition={Partition}, Offset={Offset}",
                            result.Partition,
                            result.Offset);

                        continue;
                    }

                    using var scope =
                        scopeFactory.CreateScope();

                    var processor =
                        scope.ServiceProvider
                            .GetRequiredService<ITransactionProcessor>();

                    var processResult =
                        await processor.ProcessAsync(
                            message,
                            stoppingToken);

                    logger.LogInformation(
                        "Transaction processed. " +
                        "TransactionId={TransactionId}, " +
                        "MerchantId={MerchantId}, " +
                        "Result={Result}, " +
                        "Partition={Partition}, " +
                        "Offset={Offset}",
                        message.TransactionId,
                        message.MerchantId,
                        processResult,
                        result.Partition,
                        result.Offset);

                    // IMPORTANT:
                    // DB transaction has already committed
                    // before we commit the Kafka offset.
                    consumer.Commit(result);

                    logger.LogInformation(
                        "Kafka offset committed. " +
                        "Partition={Partition}, Offset={Offset}",
                        result.Partition,
                        result.Offset);
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