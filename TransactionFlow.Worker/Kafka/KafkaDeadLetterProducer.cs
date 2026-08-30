using Confluent.Kafka;
using Microsoft.Extensions.Options;
using System.Text.Json;
using TransactionFlow.Contracts.DeadLetter;

namespace TransactionFlow.Worker.Kafka;

public sealed class KafkaDeadLetterProducer(
    IOptions<KafkaOptions> options,
    IProducer<string, string> producer,
    ILogger<KafkaDeadLetterProducer> logger)
    : IDeadLetterProducer
{
    private static readonly JsonSerializerOptions JsonOptions = new(
        JsonSerializerDefaults.Web);

    public async Task PublishAsync(
        ConsumeResult<string, string> result,
        Exception exception,
        CancellationToken cancellationToken)
    {
        var deadLetter = new DeadLetterMessage(
            OriginalTopic: result.Topic,
            OriginalPartition: result.Partition.Value,
            OriginalOffset: result.Offset.Value,
            ErrorType: exception.GetType().Name,
            ErrorMessage: exception.Message,
            Payload: result.Message.Value,
            FailedAt: DateTimeOffset.UtcNow);

        var payload =
            JsonSerializer.Serialize(
                deadLetter,
                JsonOptions);

        var message = new Message<string, string>
        {
            Key = result.Message.Key,
            Value = payload
        };

        var deliveryResult =
            await producer.ProduceAsync(
                options.Value.DeadLetterTopic,
                message,
                cancellationToken);

        logger.LogWarning(
            "Message moved to DLQ. " +
            "OriginalTopic={Topic}, " +
            "Partition={Partition}, " +
            "Offset={Offset}, " +
            "DLQTopic={DlqTopic}, " +
            "DLQPartition={DlqPartition}, " +
            "DLQOffset={DlqOffset}",
            result.Topic,
            result.Partition,
            result.Offset,
            deliveryResult.Topic,
            deliveryResult.Partition,
            deliveryResult.Offset);
    }
}