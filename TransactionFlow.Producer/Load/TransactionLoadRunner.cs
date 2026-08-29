using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransactionFlow.Contracts;
using TransactionFlow.Producer.Kafka;
using TransactionFlow.Producer.Transactions;

namespace TransactionFlow.Producer.Load;

public sealed class TransactionLoadRunner
{
    private readonly ITransactionProducer _producer;
    private readonly TransactionGenerator _generator;
    private readonly LoadOptions _options;
    private readonly ILogger<TransactionLoadRunner> _logger;

    public TransactionLoadRunner(
        ITransactionProducer producer,
        TransactionGenerator generator,
        IOptions<LoadOptions> options,
        ILogger<TransactionLoadRunner> logger)
    {
        _producer = producer;
        _generator = generator;
        _options = options.Value;
        _logger = logger;
    }

    public async Task RunAsync(
        CancellationToken cancellationToken)
    {
        var delay =
            TimeSpan.FromSeconds(
                1.0 / _options.Rate);

        var generated =
            new List<TransactionMessage>();

        var stopwatch =
            System.Diagnostics.Stopwatch.StartNew();

        for (var i = 0; i < _options.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var transaction =
                _generator.Generate();

            generated.Add(transaction);

            await _producer.PublishAsync(
                transaction,
                cancellationToken);

            if ((i + 1) % _options.Rate == 0)
            {
                _logger.LogInformation(
                    "Produced {Count}/{Total} transactions",
                    i + 1,
                    _options.Count);
            }

            await Task.Delay(
                delay,
                cancellationToken);
        }

        stopwatch.Stop();

        _logger.LogInformation(
            "Load completed. Count={Count}, Duration={Duration}",
            _options.Count,
            stopwatch.Elapsed);
    }
}