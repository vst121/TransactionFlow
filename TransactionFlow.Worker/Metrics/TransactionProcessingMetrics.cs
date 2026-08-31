namespace TransactionFlow.Worker.Metrics;

public sealed class TransactionProcessingMetrics
{
    private long _consumedCount;
    private long _processedCount;
    private long _duplicateCount;
    private long _ignoredCount;

    private long _processingDurationTicks;
    private long _processingDurationCount;
    private long _minProcessingDurationTicks = long.MaxValue;
    private long _maxProcessingDurationTicks;

    public void IncrementConsumed() =>
        Interlocked.Increment(ref _consumedCount);

    public void IncrementProcessed() =>
        Interlocked.Increment(ref _processedCount);

    public void IncrementDuplicate() =>
        Interlocked.Increment(ref _duplicateCount);

    public void IncrementIgnored() =>
        Interlocked.Increment(ref _ignoredCount);

    public void RecordProcessingDuration(
        TimeSpan duration)
    {
        var ticks = duration.Ticks;

        Interlocked.Increment(
            ref _processingDurationCount);

        Interlocked.Add(
            ref _processingDurationTicks,
            ticks);

        UpdateMin(ticks);
        UpdateMax(ticks);
    }

    public long ConsumedCount =>
        Interlocked.Read(ref _consumedCount);

    public long ProcessedCount =>
        Interlocked.Read(ref _processedCount);

    public long DuplicateCount =>
        Interlocked.Read(ref _duplicateCount);

    public long IgnoredCount =>
        Interlocked.Read(ref _ignoredCount);

    public long ProcessingDurationCount =>
        Interlocked.Read(ref _processingDurationCount);

    public TimeSpan AverageProcessingDuration
    {
        get
        {
            var count =
                Interlocked.Read(
                    ref _processingDurationCount);

            if (count == 0)
            {
                return TimeSpan.Zero;
            }

            var ticks =
                Interlocked.Read(
                    ref _processingDurationTicks);

            return TimeSpan.FromTicks(
                ticks / count);
        }
    }

    public TimeSpan MinProcessingDuration
    {
        get
        {
            var count =
                Interlocked.Read(
                    ref _processingDurationCount);

            if (count == 0)
            {
                return TimeSpan.Zero;
            }

            var ticks =
                Interlocked.Read(
                    ref _minProcessingDurationTicks);

            return TimeSpan.FromTicks(ticks);
        }
    }

    public TimeSpan MaxProcessingDuration
    {
        get
        {
            var count =
                Interlocked.Read(
                    ref _processingDurationCount);

            if (count == 0)
            {
                return TimeSpan.Zero;
            }

            var ticks =
                Interlocked.Read(
                    ref _maxProcessingDurationTicks);

            return TimeSpan.FromTicks(ticks);
        }
    }

    private void UpdateMin(long value)
    {
        while (true)
        {
            var current =
                Interlocked.Read(
                    ref _minProcessingDurationTicks);

            if (value >= current)
            {
                return;
            }

            if (Interlocked.CompareExchange(
                    ref _minProcessingDurationTicks,
                    value,
                    current) == current)
            {
                return;
            }
        }
    }

    private void UpdateMax(long value)
    {
        while (true)
        {
            var current =
                Interlocked.Read(
                    ref _maxProcessingDurationTicks);

            if (value <= current)
            {
                return;
            }

            if (Interlocked.CompareExchange(
                    ref _maxProcessingDurationTicks,
                    value,
                    current) == current)
            {
                return;
            }
        }
    }
}
