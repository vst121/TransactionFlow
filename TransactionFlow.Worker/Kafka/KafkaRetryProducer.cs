using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using TransactionFlow.Contracts.Retry;

namespace TransactionFlow.Worker.Kafka;

public sealed class KafkaRetryProducer(
    IOptions<KafkaOptions> options,
    IProducer<string, string> producer,
    ILogger<KafkaRetryProducer> logger)
    : IRetryProducer
{
    private static readonly JsonSerializerOptions JsonOptions = new(
        JsonSerializerDefaults.Web);

    public async Task PublishAsync(
        string key,
        RetryMessage retryMessage,
        CancellationToken cancellationToken)
    {
        var payload =
            JsonSerializer.Serialize(
                retryMessage,
                JsonOptions);

        var message = new Message<string, string>
        {
            Key = key,
            Value = payload
        };

        var deliveryResult =
            await producer.ProduceAsync(
                options.Value.RetryTopic,
                message,
                cancellationToken);

        logger.LogWarning(
            "Message moved to retry topic. " +
            "OriginalTopic={OriginalTopic}, " +
            "OriginalPartition={OriginalPartition}, " +
            "OriginalOffset={OriginalOffset}, " +
            "TransactionId={TransactionId}, " +
            "Attempt={Attempt}, " +
            "RetryTopic={RetryTopic}, " +
            "RetryPartition={RetryPartition}, " +
            "RetryOffset={RetryOffset}",
            retryMessage.OriginalTopic,
            retryMessage.OriginalPartition,
            retryMessage.OriginalOffset,
            retryMessage.TransactionId,
            retryMessage.Attempt,
            deliveryResult.Topic,
            deliveryResult.Partition,
            deliveryResult.Offset);
    }
}
