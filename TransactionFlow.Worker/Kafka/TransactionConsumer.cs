using Confluent.Kafka;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using System.Text.Json;
using TransactionFlow.Application.Common.Errors;
using TransactionFlow.Application.Transactions;
using TransactionFlow.Contracts.Retry;
using TransactionFlow.Contracts.Transactions;
using TransactionFlow.Worker.Metrics;

namespace TransactionFlow.Worker.Kafka;

public sealed class TransactionConsumer(
    IOptions<KafkaOptions> options,
    IOptions<FailureInjectionOptions> failureInjection,
    IServiceScopeFactory scopeFactory,
    IDeadLetterProducer deadLetterProducer,
    TransactionValidator validator,
    TransactionProcessingMetrics metrics,
    ILogger<TransactionConsumer> logger)
    : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly KafkaOptions _options = options.Value;

    private readonly FailureInjectionOptions _failureInjection =
        failureInjection.Value;

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
                .SetPartitionsAssignedHandler(
                    (_, partitions) =>
                    {
                        logger.LogInformation(
                            "Partitions assigned: {Partitions}",
                            string.Join(
                                ", ",
                                partitions.Select(p =>
                                    $"{p.Topic}[{p.Partition}]")));
                    })
                .SetPartitionsRevokedHandler(
                    (_, partitions) =>
                    {
                        logger.LogInformation(
                            "Partitions revoked: {Partitions}",
                            string.Join(
                                ", ",
                                partitions.Select(p =>
                                    $"{p.Topic}[{p.Partition}]")));
                    })
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

                    metrics.IncrementConsumed();

                    logger.LogDebug(
                        "Kafka message received. " +
                        "Topic={Topic}, " +
                        "Partition={Partition}, " +
                        "Offset={Offset}",
                        result.Topic,
                        result.Partition,
                        result.Offset);

                    // -------------------------------------------------
                    // 1. Deserialize
                    // -------------------------------------------------

                    TransactionMessage? message;

                    try
                    {
                        if (string.IsNullOrWhiteSpace(
                                result.Message.Value))
                        {
                            throw new JsonException(
                                "Kafka message payload is empty.");
                        }

                        message =
                            JsonSerializer.Deserialize<TransactionMessage>(
                                result.Message.Value,
                                JsonOptions);
                    }
                    catch (JsonException ex)
                    {
                        logger.LogError(
                            ex,
                            "Invalid transaction JSON. " +
                            "Sending message to DLQ. " +
                            "Partition={Partition}, Offset={Offset}",
                            result.Partition,
                            result.Offset);

                        await deadLetterProducer.PublishAsync(
                            result,
                            ex,
                            stoppingToken);

                        CommitOffset(
                            consumer,
                            result);

                        logger.LogWarning(
                            "Invalid JSON sent to DLQ and offset committed. " +
                            "Partition={Partition}, Offset={Offset}",
                            result.Partition,
                            result.Offset);

                        continue;
                    }

                    if (message is null)
                    {
                        var exception =
                            new InvalidTransactionException(
                                "Message deserialized to null.");

                        await deadLetterProducer.PublishAsync(
                            result,
                            exception,
                            stoppingToken);

                        CommitOffset(
                            consumer,
                            result);

                        logger.LogWarning(
                            "Null transaction sent to DLQ. " +
                            "Partition={Partition}, Offset={Offset}",
                            result.Partition,
                            result.Offset);

                        continue;
                    }

                    // -------------------------------------------------
                    // 2. Validate
                    // -------------------------------------------------

                    var validation =
                        validator.Validate(message);

                    if (!validation.IsValid)
                    {
                        var exception =
                            new InvalidTransactionException(
                                validation.Error!);

                        logger.LogWarning(
                            "Invalid transaction. " +
                            "TransactionId={TransactionId}, " +
                            "Reason={Reason}. " +
                            "Sending to DLQ.",
                            message.TransactionId,
                            validation.Error);

                        await deadLetterProducer.PublishAsync(
                            result,
                            exception,
                            stoppingToken);

                        CommitOffset(
                            consumer,
                            result);

                        logger.LogInformation(
                            "Invalid transaction sent to DLQ " +
                            "and offset committed. " +
                            "TransactionId={TransactionId}, " +
                            "Partition={Partition}, " +
                            "Offset={Offset}",
                            message.TransactionId,
                            result.Partition,
                            result.Offset);

                        continue;
                    }

                    // -------------------------------------------------
                    // 3. Application processing
                    // -------------------------------------------------

                    using var scope =
                        scopeFactory.CreateScope();

                    var processingService =
                        scope.ServiceProvider
                            .GetRequiredService<
                                ITransactionProcessingService>();

                    TransactionProcessingOutcome processingOutcome;

                    var processingStopwatch =
                        Stopwatch.StartNew();

                    try
                    {
                        try
                        {
                            processingOutcome =
                                await processingService.ProcessAsync(
                                    message,
                                    stoppingToken);
                        }
                        catch (Exception ex)
                        {
                            // TransactionProcessingService has already
                            // performed its local retry attempts.
                            // At this point the processing has definitively
                            // failed and the message must go to the DLQ.

                            logger.LogError(
                                ex,
                                "Transaction processing failed after retry attempts. " +
                                "TransactionId={TransactionId}, " +
                                "Partition={Partition}, " +
                                "Offset={Offset}",
                                message.TransactionId,
                                result.Partition,
                                result.Offset);

                            await deadLetterProducer.PublishAsync(
                                result,
                                ex,
                                stoppingToken);

                            CommitOffset(
                                consumer,
                                result);

                            logger.LogWarning(
                                "Transaction sent to DLQ and offset committed. " +
                                "TransactionId={TransactionId}, " +
                                "Partition={Partition}, " +
                                "Offset={Offset}",
                                message.TransactionId,
                                result.Partition,
                                result.Offset);

                            continue;
                        }
                    }
                    finally
                    {
                        processingStopwatch.Stop();

                        metrics.RecordProcessingDuration(
                            processingStopwatch.Elapsed);
                    }


                    // -------------------------------------------------
                    // 4. Update metrics
                    // -------------------------------------------------

                    switch (processingOutcome)
                    {
                        case TransactionProcessingOutcome.Processed:
                            metrics.IncrementProcessed();
                            break;

                        case TransactionProcessingOutcome.Duplicate:
                            metrics.IncrementDuplicate();
                            break;

                        case TransactionProcessingOutcome.Ignored:
                            metrics.IncrementIgnored();
                            break;

                        default:
                            logger.LogWarning(
                                "Unknown transaction processing outcome: {Outcome}",
                                processingOutcome);
                            break;
                    }

                    logger.LogInformation(
                        "Transaction processed. " +
                        "TransactionId={TransactionId}, " +
                        "MerchantId={MerchantId}, " +
                        "Outcome={Outcome}, " +
                        "Partition={Partition}, " +
                        "Offset={Offset}",
                        message.TransactionId,
                        message.MerchantId,
                        processingOutcome,
                        result.Partition,
                        result.Offset);

                    // -------------------------------------------------
                    // 5. Failure injection
                    //
                    // Database transaction has already committed.
                    // Kafka offset has NOT been committed yet.
                    // -------------------------------------------------

                    if (_failureInjection.CrashAfterDatabaseCommit)
                    {
                        logger.LogCritical(
                            "FAILURE INJECTION: crashing after DB commit " +
                            "before Kafka offset commit.");

                        Environment.FailFast(
                            "Failure injection: crash after database commit.");
                    }

                    // -------------------------------------------------
                    // 6. Commit Kafka offset
                    // -------------------------------------------------

                    CommitOffset(
                        consumer,
                        result);

                    logger.LogDebug(
                        "Kafka offset committed. " +
                        "TransactionId={TransactionId}, " +
                        "Partition={Partition}, " +
                        "Offset={Offset}",
                        message.TransactionId,
                        result.Partition,
                        result.Offset);
                }
                catch (ConsumeException ex)
                {
                    logger.LogError(
                        ex,
                        "Kafka consume failed. " +
                        "Reason={Reason}",
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

    private void CommitOffset(
    IConsumer<string, string> consumer,
    ConsumeResult<string, string> result)
    {
        try
        {
            consumer.Commit(result);

            logger.LogDebug(
                "Kafka offset committed. " +
                "Topic={Topic}, " +
                "Partition={Partition}, " +
                "Offset={Offset}",
                result.Topic,
                result.Partition,
                result.Offset);
        }
        catch (KafkaException ex)
        {
            logger.LogError(
                ex,
                "Failed to commit Kafka offset. " +
                "Topic={Topic}, " +
                "Partition={Partition}, " +
                "Offset={Offset}",
                result.Topic,
                result.Partition,
                result.Offset);

            throw;
        }
    }
}

