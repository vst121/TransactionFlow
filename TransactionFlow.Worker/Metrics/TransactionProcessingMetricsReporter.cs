namespace TransactionFlow.Worker.Metrics;

public sealed class TransactionProcessingMetricsReporter(
    TransactionProcessingMetrics metrics,
    ILogger<TransactionProcessingMetricsReporter> logger)
{
    private long _lastConsumed;
    private long _lastProcessed;

    private DateTimeOffset _lastSnapshotAt =
        DateTimeOffset.UtcNow;

    private readonly DateTimeOffset _startedAt =
        DateTimeOffset.UtcNow;

    public void Report()
    {
        var now = DateTimeOffset.UtcNow;

        var consumed = metrics.ConsumedCount;
        var processed = metrics.ProcessedCount;
        var duplicate = metrics.DuplicateCount;
        var ignored = metrics.IgnoredCount;

        // Do not produce noisy logs when nothing new happened.
        if (consumed == _lastConsumed)
        {
            return;
        }

        var elapsedSinceLastSnapshot =
            now - _lastSnapshotAt;

        var consumedSinceLastSnapshot =
            consumed - _lastConsumed;

        var processedSinceLastSnapshot =
            processed - _lastProcessed;

        var consumedRate =
            elapsedSinceLastSnapshot.TotalSeconds > 0
                ? consumedSinceLastSnapshot /
                  elapsedSinceLastSnapshot.TotalSeconds
                : 0;

        var businessRate =
            elapsedSinceLastSnapshot.TotalSeconds > 0
                ? processedSinceLastSnapshot /
                  elapsedSinceLastSnapshot.TotalSeconds
                : 0;

        var totalElapsed =
            now - _startedAt;

        var averageConsumedRate =
            totalElapsed.TotalSeconds > 0
                ? consumed /
                  totalElapsed.TotalSeconds
                : 0;

        var averageProcessingDuration =
            metrics.AverageProcessingDuration;

        var minProcessingDuration =
            metrics.MinProcessingDuration;

        var maxProcessingDuration =
            metrics.MaxProcessingDuration;

        logger.LogInformation(
            "Worker metrics: " +
            "Consumed={Consumed}, " +
            "Processed={Processed}, " +
            "Duplicate={Duplicate}, " +
            "Ignored={Ignored}, " +
            "ConsumedRate={ConsumedRate:F2} msg/s, " +
            "BusinessRate={BusinessRate:F2} tx/s, " +
            "AvgProcessing={AvgProcessing:F2} ms, " +
            "MinProcessing={MinProcessing:F2} ms, " +
            "MaxProcessing={MaxProcessing:F2} ms, " +
            "AverageConsumedRate={AverageConsumedRate:F2} msg/s",
            consumed,
            processed,
            duplicate,
            ignored,
            consumedRate,
            businessRate,
            averageProcessingDuration.TotalMilliseconds,
            minProcessingDuration.TotalMilliseconds,
            maxProcessingDuration.TotalMilliseconds,
            averageConsumedRate);

        _lastConsumed = consumed;
        _lastProcessed = processed;
        _lastSnapshotAt = now;
    }
}
