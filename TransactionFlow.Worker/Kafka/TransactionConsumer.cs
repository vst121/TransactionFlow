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
    IRetryProducer retryProducer,
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
    private DateTimeOffset _lastMetricsAt = DateTimeOffset.UtcNow;

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

        // Start metrics loop
        var metricsTask =
            LogMetricsPeriodicallyAsync(stoppingToken);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    logger.LogDebug(
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

                    logger.LogDebug(
                        "Raw Kafka value: {Value}",
                        result.Message.Value);

                    // -------------------------------------------------
                    // 1. Deserialize
                    // -------------------------------------------------

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
                            "Invalid JSON. Sending message to DLQ. " +
                            "Partition={Partition}, Offset={Offset}",
                            result.Partition,
                            result.Offset);

                        await deadLetterProducer.PublishAsync(
                            result,
                            ex,
                            stoppingToken);

                        consumer.Commit(result);

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

                        logger.LogWarning(
                            "Message deserialized to null. " +
                            "Sending to DLQ. " +
                            "Partition={Partition}, Offset={Offset}",
                            result.Partition,
                            result.Offset);

                        await deadLetterProducer.PublishAsync(
                            result,
                            exception,
                            stoppingToken);

                        consumer.Commit(result);

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
                            "Reason={Reason}",
                            message.TransactionId,
                            validation.Error);

                        await deadLetterProducer.PublishAsync(
                            result,
                            exception,
                            stoppingToken);

                        consumer.Commit(result);

                        logger.LogInformation(
                            "Invalid transaction sent to DLQ " +
                            "and offset committed. " +
                            "Partition={Partition}, Offset={Offset}",
                            result.Partition,
                            result.Offset);

                        continue;
                    }

                    // -------------------------------------------------
                    // 3. Business processing
                    // -------------------------------------------------

                    using var scope =
                        scopeFactory.CreateScope();

                    var processingService =
                        scope.ServiceProvider
                            .GetRequiredService<ITransactionProcessingService>();

                    try
                    {
                        var processResult =
                            await processingService.ProcessAsync(
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

                        switch (processResult)
                        {
                            case TransactionProcessingOutcome.Processed:
                                Interlocked.Increment(ref _processedCount);
                                break;

                            case TransactionProcessingOutcome.Duplicate:
                                Interlocked.Increment(ref _duplicateCount);
                                break;

                            case TransactionProcessingOutcome.Ignored:
                                Interlocked.Increment(ref _ignoredCount);
                                break;

                            default:
                                logger.LogWarning(
                                    "Unknown transaction processing outcome: {Outcome}",
                                    processResult);
                                break;
                        }

                        // -------------------------------------------------
                        // 4. Failure injection
                        //
                        // DB transaction has already committed.
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
                        // 5. Commit Kafka offset
                        // -------------------------------------------------

                        consumer.Commit(result);

                        logger.LogInformation(
                            "Kafka offset committed. " +
                            "TransactionId={TransactionId}, " +
                            "Partition={Partition}, " +
                            "Offset={Offset}",
                            message.TransactionId,
                            result.Partition,
                            result.Offset);
                    }
                    catch (Exception ex)
                    {
                        var errorKind =
                            errorClassifier.Classify(ex);

                        logger.LogError(
                            ex,
                            "Transaction processing failed. " +
                            "TransactionId={TransactionId}, " +
                            "ErrorKind={ErrorKind}",
                            message.TransactionId,
                            errorKind);

                        // -------------------------------------------------
                        // 6. Transient failure → Retry
                        // -------------------------------------------------

                        if (errorKind == ErrorKind.Transient)
                        {
                            var retryMessage =
                                new RetryMessage(
                                    OriginalTopic: result.Topic,
                                    OriginalPartition: result.Partition.Value,
                                    OriginalOffset: result.Offset.Value,
                                    TransactionId: message.TransactionId,
                                    Attempt: 1,
                                    ErrorType: ex.GetType().Name,
                                    ErrorMessage: ex.Message,
                                    Payload: result.Message.Value,
                                    FailedAt: DateTimeOffset.UtcNow);

                            await retryProducer.PublishAsync(
                                result.Message.Key,
                                retryMessage,
                                stoppingToken);

                            consumer.Commit(result);

                            logger.LogWarning(
                                "Transient failure sent to retry topic " +
                                "and original offset committed. " +
                                "TransactionId={TransactionId}, " +
                                "Partition={Partition}, " +
                                "Offset={Offset}, " +
                                "Attempt={Attempt}",
                                message.TransactionId,
                                result.Partition,
                                result.Offset,
                                retryMessage.Attempt);

                            continue;
                        }

                        // -------------------------------------------------
                        // 7. Permanent failure → DLQ
                        // -------------------------------------------------

                        await deadLetterProducer.PublishAsync(
                            result,
                            ex,
                            stoppingToken);

                        consumer.Commit(result);

                        logger.LogWarning(
                            "Permanent failure sent to DLQ " +
                            "and original offset committed. " +
                            "TransactionId={TransactionId}, " +
                            "Partition={Partition}, " +
                            "Offset={Offset}",
                            message.TransactionId,
                            result.Partition,
                            result.Offset);

                        continue;
                    }
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

    private void LogMetrics()
    {
        var now = DateTimeOffset.UtcNow;

        var processed =
            Interlocked.Read(ref _processedCount);

        var duplicates =
            Interlocked.Read(ref _duplicateCount);

        var ignored =
            Interlocked.Read(ref _ignoredCount);

        var elapsed =
            now - _lastMetricsAt;

        var total =
            processed +
            duplicates +
            ignored;

        var processedSinceLast =
            processed - _lastProcessedCount;

        var throughput =
            elapsed.TotalSeconds > 0
                ? processedSinceLast / elapsed.TotalSeconds
                : 0;

        logger.LogInformation(
            "Worker metrics: " +
            "Total={Total}, " +
            "Processed={Processed}, " +
            "Duplicate={Duplicate}, " +
            "Ignored={Ignored}, " +
            "Throughput={Throughput:F2} tx/s",
            total,
            processed,
            duplicates,
            ignored,
            throughput);

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
}

