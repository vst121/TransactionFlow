using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace TransactionFlow.Worker.Telemetry;

public static class TransactionFlowTelemetry
{
    public const string ServiceName =
        "TransactionFlow.Worker";

    public static readonly ActivitySource ActivitySource =
        new(ServiceName);

    public static readonly Meter Meter =
        new(ServiceName);

    public static readonly Counter<long> ConsumedMessages =
        Meter.CreateCounter<long>(
            name: "transactionflow.kafka.messages.consumed",
            unit: "{message}",
            description: "Number of Kafka messages consumed.");

    public static readonly Counter<long> ProcessedTransactions =
        Meter.CreateCounter<long>(
            name: "transactionflow.transactions.processed",
            unit: "{transaction}",
            description: "Number of successfully processed transactions.");

    public static readonly Counter<long> DuplicateTransactions =
        Meter.CreateCounter<long>(
            name: "transactionflow.transactions.duplicates",
            unit: "{transaction}",
            description: "Number of duplicate transactions.");

    public static readonly Counter<long> IgnoredTransactions =
        Meter.CreateCounter<long>(
            name: "transactionflow.transactions.ignored",
            unit: "{transaction}",
            description: "Number of ignored transactions.");

    public static readonly Counter<long> DeadLetteredMessages =
        Meter.CreateCounter<long>(
            name: "transactionflow.kafka.messages.dead_lettered",
            unit: "{message}",
            description: "Number of messages sent to the dead-letter queue.");

    public static readonly Histogram<double> ProcessingDuration =
        Meter.CreateHistogram<double>(
            name: "transactionflow.transaction.processing.duration",
            unit: "ms",
            description: "Transaction processing duration.");
}
