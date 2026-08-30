using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using TransactionFlow.Application.Common.Errors;
using TransactionFlow.Application.Transactions;
using TransactionFlow.Contracts.Retry;
using TransactionFlow.Contracts.Transactions;

namespace TransactionFlow.Worker.Kafka;

public sealed class TransactionRetryConsumer(
    IOptions<KafkaOptions> options,
    IServiceScopeFactory scopeFactory,
    IRetryProducer retryProducer,
    IDeadLetterProducer deadLetterProducer,
    IErrorClassifier errorClassifier,
    ILogger<TransactionRetryConsumer> logger)
    : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(
        JsonSerializerDefaults.Web);

    private const int MaxAttempts = 3;

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            GroupId = $"{options.Value.GroupId}-retry",

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
                        "Retry Kafka error: {Reason}",
                        error.Reason);
                })
                .Build();

        consumer.Subscribe(options.Value.RetryTopic);

        logger.LogInformation(
            "Retry consumer started. Topic={Topic}, GroupId={GroupId}",
            options.Value.RetryTopic,
            $"{options.Value.GroupId}-retry");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result =
                        consumer.Consume(stoppingToken);

                    logger.LogInformation(
                        "Retry message received. " +
                        "Topic={Topic}, " +
                        "Partition={Partition}, " +
                        "Offset={Offset}",
                        result.Topic,
                        result.Partition,
                        result.Offset);

                    RetryMessage? retryMessage;

                    try
                    {
                        retryMessage =
                            JsonSerializer.Deserialize<RetryMessage>(
                                result.Message.Value,
                                JsonOptions);
                    }
                    catch (JsonException ex)
                    {
                        logger.LogError(
                            ex,
                            "Invalid retry message. " +
                            "Partition={Partition}, Offset={Offset}",
                            result.Partition,
                            result.Offset);

                        await deadLetterProducer.PublishAsync(
                            result,
                            ex,
                            stoppingToken);

                        consumer.Commit(result);

                        continue;
                    }

                    if (retryMessage is null)
                    {
                        logger.LogError(
                            "Retry message deserialized to null. " +
                            "Partition={Partition}, Offset={Offset}",
                            result.Partition,
                            result.Offset);

                        consumer.Commit(result);

                        continue;
                    }

                    TransactionMessage? transaction;

                    try
                    {
                        transaction =
                            JsonSerializer.Deserialize<TransactionMessage>(
                                retryMessage.Payload,
                                JsonOptions);
                    }
                    catch (JsonException ex)
                    {
                        logger.LogError(
                            ex,
                            "Original transaction payload is invalid. " +
                            "TransactionId={TransactionId}, " +
                            "Attempt={Attempt}",
                            retryMessage.TransactionId,
                            retryMessage.Attempt);

                        await deadLetterProducer.PublishAsync(
                            result,
                            ex,
                            stoppingToken);

                        consumer.Commit(result);

                        continue;
                    }

                    if (transaction is null)
                    {
                        logger.LogError(
                            "Original transaction payload deserialized to null. " +
                            "TransactionId={TransactionId}",
                            retryMessage.TransactionId);

                        consumer.Commit(result);

                        continue;
                    }

                    using var scope =
                        scopeFactory.CreateScope();

                    var processingService =
                        scope.ServiceProvider
                            .GetRequiredService<ITransactionProcessingService>();

                    try
                    {
                        var processResult =
                            await processingService.ProcessAsync(
                                transaction,
                                stoppingToken);

                        logger.LogInformation(
                            "Retry processing succeeded. " +
                            "TransactionId={TransactionId}, " +
                            "Attempt={Attempt}, " +
                            "Result={Result}",
                            transaction.TransactionId,
                            retryMessage.Attempt,
                            processResult);

                        consumer.Commit(result);
                    }
                    catch (Exception ex)
                    {
                        await HandleFailureAsync(
                            consumer,
                            result,
                            retryMessage,
                            ex,
                            stoppingToken);
                    }
                }
                catch (ConsumeException ex)
                {
                    logger.LogError(
                        ex,
                        "Retry Kafka consume failed: {Reason}",
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

    private async Task HandleFailureAsync(
        IConsumer<string, string> consumer,
        ConsumeResult<string, string> result,
        RetryMessage retryMessage,
        Exception exception,
        CancellationToken cancellationToken)
    {
        var errorKind =
            errorClassifier.Classify(exception);

        // Permanent failure -> DLQ immediately
        if (errorKind == ErrorKind.Permanent)
        {
            await deadLetterProducer.PublishAsync(
                result,
                exception,
                cancellationToken);

            consumer.Commit(result);

            logger.LogWarning(
                "Permanent failure. " +
                "TransactionId={TransactionId}, " +
                "Attempt={Attempt}. Sent to DLQ.",
                retryMessage.TransactionId,
                retryMessage.Attempt);

            return;
        }

        // We already reached the maximum number of attempts.
        if (retryMessage.Attempt >= MaxAttempts)
        {
            await deadLetterProducer.PublishAsync(
                result,
                exception,
                cancellationToken);

            consumer.Commit(result);

            logger.LogError(
                "Maximum retry attempts reached. " +
                "TransactionId={TransactionId}, " +
                "Attempt={Attempt}. Sent to DLQ.",
                retryMessage.TransactionId,
                retryMessage.Attempt);

            return;
        }

        var nextAttempt =
            retryMessage.Attempt + 1;

        var nextRetryMessage =
            new RetryMessage(
                OriginalTopic: retryMessage.OriginalTopic,
                OriginalPartition: retryMessage.OriginalPartition,
                OriginalOffset: retryMessage.OriginalOffset,
                TransactionId: retryMessage.TransactionId,
                Attempt: nextAttempt,
                ErrorType: exception.GetType().Name,
                ErrorMessage: exception.Message,
                Payload: retryMessage.Payload,
                FailedAt: DateTimeOffset.UtcNow);

        await retryProducer.PublishAsync(
            result.Message.Key,
            nextRetryMessage,
            cancellationToken);

        consumer.Commit(result);

        logger.LogWarning(
            "Transient failure. " +
            "TransactionId={TransactionId}, " +
            "Scheduled retry. Attempt={Attempt}",
            retryMessage.TransactionId,
            nextAttempt);
    }
}