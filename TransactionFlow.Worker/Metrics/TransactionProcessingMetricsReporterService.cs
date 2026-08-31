namespace TransactionFlow.Worker.Metrics;

public sealed class TransactionProcessingMetricsReporterService(
    TransactionProcessingMetricsReporter reporter,
    ILogger<TransactionProcessingMetricsReporterService> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        using var timer =
            new PeriodicTimer(TimeSpan.FromSeconds(5));

        try
        {
            while (await timer.WaitForNextTickAsync(
                       stoppingToken))
            {
                reporter.Report();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown.
        }
    }
}
