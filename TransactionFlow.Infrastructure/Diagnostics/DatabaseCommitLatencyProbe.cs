using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using TransactionFlow.Infrastructure.Persistence;

namespace TransactionFlow.Infrastructure.Diagnostics;

public sealed class DatabaseCommitLatencyProbe(
    TransactionFlowDbContext db,
    ILogger<DatabaseCommitLatencyProbe> logger)
{
    private const int WarmupIterations = 3;
    private const int MeasurementIterations = 20;

    public async Task RunAsync(
        CancellationToken cancellationToken)
    {
        logger.LogInformation(
            "Starting PostgreSQL commit latency probe. " +
            "Warmup={Warmup}, Measurements={Measurements}",
            WarmupIterations,
            MeasurementIterations);

        await db.Database.OpenConnectionAsync(
            cancellationToken);

        try
        {
            await RunScenarioAsync(
                synchronousCommit: true,
                cancellationToken);

            await RunScenarioAsync(
                synchronousCommit: false,
                cancellationToken);
        }
        finally
        {
            await db.Database.ExecuteSqlRawAsync(
                "SET synchronous_commit = on;",
                cancellationToken);

            await db.Database.CloseConnectionAsync();
        }

        logger.LogInformation(
            "PostgreSQL commit latency probe completed.");
    }

    private async Task RunScenarioAsync(
        bool synchronousCommit,
        CancellationToken cancellationToken)
    {
        var mode =
            synchronousCommit
                ? "ON"
                : "OFF";

        await db.Database.ExecuteSqlRawAsync(
            $"SET synchronous_commit = {mode};",
            cancellationToken);

        logger.LogInformation(
            "Testing synchronous_commit={Mode}",
            mode);

        // ---------------------------------------------
        // Warmup
        // ---------------------------------------------

        for (var i = 0; i < WarmupIterations; i++)
        {
            await ExecuteInsertOnlyAsync(
                cancellationToken);
        }

        // ---------------------------------------------
        // Measurement
        // ---------------------------------------------

        var measurements =
            new List<double>(
                MeasurementIterations);

        for (var i = 0; i < MeasurementIterations; i++)
        {
            var commitMs =
                await ExecuteInsertOnlyAsync(
                    cancellationToken);

            measurements.Add(commitMs);

            logger.LogDebug(
                "synchronous_commit={Mode}, " +
                "Iteration={Iteration}, " +
                "CommitMs={CommitMs:F2}",
                mode,
                i + 1,
                commitMs);
        }

        LogStatistics(
            mode,
            measurements);
    }

    private async Task<double> ExecuteInsertOnlyAsync(
        CancellationToken cancellationToken)
    {
        var transactionId =
            $"COMMIT-PROBE-{Guid.NewGuid():N}";

        await using var transaction =
            await db.Database.BeginTransactionAsync(
                cancellationToken);

        const string sql = """
            INSERT INTO processed_transactions
            (
                transaction_id,
                merchant_id,
                amount,
                currency,
                status,
                timestamp,
                processed_at
            )
            VALUES
            (
                {0},
                {1},
                {2},
                {3},
                {4},
                {5},
                {6}
            );
            """;

        await db.Database.ExecuteSqlRawAsync(
            sql,
            [
                transactionId,
                "COMMIT-PROBE",
                1.00m,
                "EUR",
                "SUCCESS",
                DateTimeOffset.UtcNow,
                DateTimeOffset.UtcNow
            ],
            cancellationToken);

        var stopwatch =
            Stopwatch.StartNew();

        await transaction.CommitAsync(
            cancellationToken);

        stopwatch.Stop();

        return stopwatch.Elapsed.TotalMilliseconds;
    }

    private void LogStatistics(
        string mode,
        IReadOnlyCollection<double> values)
    {
        var sorted =
            values
                .OrderBy(x => x)
                .ToArray();

        var average =
            sorted.Average();

        var min =
            sorted.First();

        var max =
            sorted.Last();

        var p50 =
            Percentile(sorted, 0.50);

        var p95 =
            Percentile(sorted, 0.95);

        logger.LogInformation(
            "PostgreSQL commit statistics. " +
            "synchronous_commit={Mode}, " +
            "Samples={Samples}, " +
            "Min={Min:F2} ms, " +
            "P50={P50:F2} ms, " +
            "P95={P95:F2} ms, " +
            "Max={Max:F2} ms, " +
            "Average={Average:F2} ms",
            mode,
            sorted.Length,
            min,
            p50,
            p95,
            max,
            average);
    }

    private static double Percentile(
        double[] sortedValues,
        double percentile)
    {
        if (sortedValues.Length == 0)
        {
            return 0;
        }

        var position =
            (sortedValues.Length - 1) * percentile;

        var lower =
            (int)Math.Floor(position);

        var upper =
            (int)Math.Ceiling(position);

        if (lower == upper)
        {
            return sortedValues[lower];
        }

        var weight =
            position - lower;

        return sortedValues[lower]
             + (sortedValues[upper] - sortedValues[lower]) * weight;
    }
}
