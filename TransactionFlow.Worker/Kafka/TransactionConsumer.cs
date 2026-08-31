using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using TransactionFlow.Application.Common.Errors;
using TransactionFlow.Application.Transactions;
using TransactionFlow.Contracts.Retry;
using TransactionFlow.Contracts.Transactions;

namespace TransactionFlow.Worker.Kafka;

public sealed class TransactionConsumer(
    IOptions<KafkaOptions> options,
    IOptions<FailureInjectionOptions> failureInjection,
    IServiceScopeFactory scopeFactory,
    IDeadLetterProducer deadLetterProducer,
    IErrorClassifier errorClassifier,
    TransactionValidator validator,
    ILogger<TransactionConsumer> logger)
    : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly KafkaOptions _options = options.Value;

    private long _processedCount;
    private long _duplicateCount;
    private long _ignoredCount;
    private long _lastProcessedCount;
    private long _lastTotalCount;
    private DateTimeOffset _lastMetricsAt = DateTimeOffset.UtcNow;
    private DateTimeOffset _metricsStartedAt = DateTimeOffset.UtcNow;

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

        var metricsTask =
            LogMetricsPeriodicallyAsync(stoppingToken);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result =
                        consumer.Consume(stoppingToken);

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

                    // -------------------------------------------------
                    // 4. Update metrics
                    // -------------------------------------------------

                    switch (processingOutcome)
                    {
                        case TransactionProcessingOutcome.Processed:
                            Interlocked.Increment(
                                ref _processedCount);
                            break;

                        case TransactionProcessingOutcome.Duplicate:
                            Interlocked.Increment(
                                ref _duplicateCount);
                            break;

                        case TransactionProcessingOutcome.Ignored:
                            Interlocked.Increment(
                                ref _ignoredCount);
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

            try
            {
                await metricsTask;
            }
            catch (OperationCanceledException)
            {
                // Expected during graceful shutdown.
            }
        }
    }

    private void LogMetrics()
    {
        var now = DateTimeOffset.UtcNow;

        var processed =
            Interlocked.Read(ref _processedCount);

        var duplicates =
            Interlocked.Read(ref _duplicateCount);

        var ignored =
            Interlocked.Read(ref _ignoredCount);

        var total =
            processed +
            duplicates +
            ignored;

        // No new messages since the last metrics snapshot.
        if (total == _lastTotalCount)
        {
            return;
        }

        var interval =
            now - _lastMetricsAt;

        var processedSinceLast =
            processed - _lastProcessedCount;

        var currentThroughput =
            interval.TotalSeconds > 0
                ? processedSinceLast / interval.TotalSeconds
                : 0;

        var totalElapsed =
            now - _metricsStartedAt;

        var averageThroughput =
            totalElapsed.TotalSeconds > 0
                ? total / totalElapsed.TotalSeconds
                : 0;

        logger.LogInformation(
            "Worker metrics: " +
            "Total={Total}, " +
            "Processed={Processed}, " +
            "Duplicate={Duplicate}, " +
            "Ignored={Ignored}, " +
            "CurrentThroughput={CurrentThroughput:F2} tx/s, " +
            "AverageThroughput={AverageThroughput:F2} tx/s",
            total,
            processed,
            duplicates,
            ignored,
            currentThroughput,
            averageThroughput);

        _lastTotalCount = total;
        _lastProcessedCount = processed;
        _lastMetricsAt = now;
    }

    private async Task LogMetricsPeriodicallyAsync(
    CancellationToken cancellationToken)
    {
        using var timer =
            new PeriodicTimer(TimeSpan.FromSeconds(5));

        try
        {
            while (await timer.WaitForNextTickAsync(
                       cancellationToken))
            {
                LogMetrics();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown.
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

