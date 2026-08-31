using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using TransactionFlow.Contracts.Transactions;
using TransactionFlow.Producer.Kafka;
using TransactionFlow.Producer.Transactions;

namespace TransactionFlow.Producer.Load;

public sealed class TransactionLoadRunner(
    ITransactionProducer producer,
    TransactionGenerator generator,
    IOptions<LoadOptions> options,
    ILogger<TransactionLoadRunner> logger)
{
    private readonly LoadOptions _options = options.Value;

    public async Task RunAsync(
        CancellationToken cancellationToken)
    {
        ValidateOptions();

        logger.LogInformation(
            "Starting load. " +
            "Count={Count}, " +
            "TargetRate={Rate} msg/s, " +
            "Concurrency={Concurrency}",
            _options.Count,
            _options.Rate,
            _options.Concurrency);

        var stopwatch = Stopwatch.StartNew();

        var inFlight =
            new List<Task<bool>>(
                _options.Concurrency);

        var produced = 0;
        var failed = 0;

        var interval =
            TimeSpan.FromSeconds(
                1.0 / _options.Rate);

        for (var i = 0; i < _options.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // -------------------------------------------------
            // Pace message generation according to target rate.
            // -------------------------------------------------

            var targetElapsed =
                TimeSpan.FromTicks(
                    interval.Ticks * i);

            var remaining =
                targetElapsed - stopwatch.Elapsed;

            if (remaining > TimeSpan.Zero)
            {
                await Task.Delay(
                    remaining,
                    cancellationToken);
            }

            var transaction =
                generator.Generate();

            inFlight.Add(
                PublishAsync(
                    transaction,
                    cancellationToken));

            // -------------------------------------------------
            // Do not allow more than the configured number
            // of concurrent publish operations.
            // -------------------------------------------------

            if (inFlight.Count >= _options.Concurrency)
            {
                var completed =
                    await Task.WhenAny(inFlight);

                inFlight.Remove(completed);

                if (await completed)
                {
                    produced++;
                }
                else
                {
                    failed++;
                }

                // A few other operations may have completed
                // while we were waiting. Collect them too.
                for (var j = inFlight.Count - 1; j >= 0; j--)
                {
                    if (!inFlight[j].IsCompleted)
                    {
                        continue;
                    }

                    var completedTask =
                        inFlight[j];

                    inFlight.RemoveAt(j);

                    if (await completedTask)
                    {
                        produced++;
                    }
                    else
                    {
                        failed++;
                    }
                }

                LogProgress(
                    produced,
                    failed);
            }
        }

        // -----------------------------------------------------
        // Wait for remaining publishes.
        // -----------------------------------------------------

        if (inFlight.Count > 0)
        {
            var results =
                await Task.WhenAll(inFlight);

            foreach (var success in results)
            {
                if (success)
                {
                    produced++;
                }
                else
                {
                    failed++;
                }
            }
        }

        stopwatch.Stop();

        var actualRate =
            produced / stopwatch.Elapsed.TotalSeconds;

        logger.LogInformation(
            "Load completed. " +
            "Requested={Requested}, " +
            "Produced={Produced}, " +
            "Failed={Failed}, " +
            "Duration={Duration}, " +
            "ActualRate={ActualRate:F2} msg/s",
            _options.Count,
            produced,
            failed,
            stopwatch.Elapsed,
            actualRate);
    }

    private async Task<bool> PublishAsync(
        TransactionMessage transaction,
        CancellationToken cancellationToken)
    {
        try
        {
            await producer.PublishAsync(
                transaction,
                cancellationToken);

            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Failed to publish transaction. " +
                "TransactionId={TransactionId}",
                transaction.TransactionId);

            return false;
        }
    }

    private void LogProgress(
        int produced,
        int failed)
    {
        var completed =
            produced + failed;

        if (completed == 0)
        {
            return;
        }

        var interval =
            _options.Count >= 10
                ? _options.Count / 10
                : 1;

        if (completed % interval != 0 &&
            completed != _options.Count)
        {
            return;
        }

        logger.LogInformation(
            "Producer progress: " +
            "Completed={Completed}/{Total}, " +
            "Produced={Produced}, " +
            "Failed={Failed}",
            completed,
            _options.Count,
            produced,
            failed);
    }

    private void ValidateOptions()
    {
        if (_options.Count <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(_options.Count),
                "Count must be greater than zero.");
        }

        if (_options.Rate <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(_options.Rate),
                "Rate must be greater than zero.");
        }

        if (_options.Concurrency <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(_options.Concurrency),
                "Concurrency must be greater than zero.");
        }
    }
}
