using Confluent.Kafka;
using Microsoft.Extensions.Options;
using System.Text.Json;
using TransactionFlow.Application.Transactions;
using TransactionFlow.Contracts;

namespace TransactionFlow.Worker.Kafka;

public sealed class TransactionConsumer(
    IOptions<KafkaOptions> options,
    IOptions<FailureInjectionOptions> failureInjection,    
    IServiceScopeFactory scopeFactory,
    ILogger<TransactionConsumer> logger)
    : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };


    private readonly KafkaOptions _options = options.Value;
    private readonly FailureInjectionOptions _failureInjection = failureInjection.Value;


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
                    logger.LogInformation(
                        "Waiting for Kafka message...");

                    var result =
                        consumer.Consume(stoppingToken);

                    logger.LogInformation(
                        "Kafka message received. " +
                        "Topic={Topic}, " +
                        "Partition={Partition}, " +
                        "Offset={Offset}",
                        result.Topic,
                        result.Partition,
                        result.Offset);

                    logger.LogInformation(
                        "Raw Kafka value: {Value}",
                        result.Message.Value);

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
                            "Invalid JSON. " +
                            "Partition={Partition}, Offset={Offset}",
                            result.Partition,
                            result.Offset);

                        continue;
                    }

                    if (message is null)
                    {
                        logger.LogError(
                            "Message deserialized to null. " +
                            "Partition={Partition}, Offset={Offset}",
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
                        "Result={Result}",
                        message.TransactionId,
                        message.MerchantId,
                        processResult);

                    if (_failureInjection.CrashAfterDatabaseCommit)
                    {
                        logger.LogCritical(
                            "FAILURE INJECTION: crashing after DB commit " +
                            "before Kafka offset commit.");

                        Environment.FailFast(
                            "Failure injection: crash after database commit.");
                    }

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