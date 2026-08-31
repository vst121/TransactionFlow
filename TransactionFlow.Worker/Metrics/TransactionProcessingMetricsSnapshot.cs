namespace TransactionFlow.Worker.Metrics;

public sealed record TransactionProcessingMetricsSnapshot(
    long Total,
    long Processed,
    long Duplicate,
    long Ignored,
    double CurrentThroughput,
    double AverageThroughput);
