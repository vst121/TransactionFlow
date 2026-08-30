namespace TransactionFlow.Worker.Kafka;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public required string BootstrapServers { get; init; }

    public required string Topic { get; init; }

    public required string GroupId { get; init; }

    public required string DeadLetterTopic { get; init; }

    public required string RetryTopic { get; init; }
}