using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using TransactionFlow.Application.Transactions;
using TransactionFlow.Domain.Transactions;
using TransactionFlow.Infrastructure.Persistence.Repositories;

namespace TransactionFlow.Infrastructure.Persistence.Transactions;

public sealed class SuccessfulTransactionHandler(
    TransactionFlowDbContext db,
    ProcessedTransactionRepository processedTransactions,
    MerchantAggregateRepository merchantAggregates,
    ILogger<SuccessfulTransactionHandler> logger)
    : ISuccessfulTransactionHandler
{
    public async Task<TransactionProcessingOutcome> HandleAsync(
        Transaction transaction,
        CancellationToken cancellationToken)
    {
        var totalStopwatch = Stopwatch.StartNew();

        await using var dbTransaction =
            await db.Database.BeginTransactionAsync(
                cancellationToken);

        try
        {
            var inserted =
                await processedTransactions.TryAddAsync(
                    transaction,
                    DateTimeOffset.UtcNow,
                    cancellationToken);

            if (!inserted)
            {
                await dbTransaction.CommitAsync(
                    cancellationToken);

                totalStopwatch.Stop();

                logger.LogDebug(
                    "Duplicate transaction detected. " +
                    "TransactionId={TransactionId}, " +
                    "TotalDbTransactionMs={TotalMs:F2}",
                    transaction.TransactionId,
                    totalStopwatch.Elapsed.TotalMilliseconds);

                return TransactionProcessingOutcome.Duplicate;
            }

            var aggregateStopwatch = Stopwatch.StartNew();

            await merchantAggregates.UpsertAsync(
                transaction,
                cancellationToken);

            aggregateStopwatch.Stop();

            var commitStopwatch = Stopwatch.StartNew();

            await dbTransaction.CommitAsync(
                cancellationToken);

            commitStopwatch.Stop();

            totalStopwatch.Stop();

            logger.LogDebug(
                "Successful transaction database operation completed. " +
                "TransactionId={TransactionId}, " +
                "AggregateMs={AggregateMs:F2}, " +
                "CommitMs={CommitMs:F2}, " +
                "TotalDbTransactionMs={TotalMs:F2}",
                transaction.TransactionId,
                aggregateStopwatch.Elapsed.TotalMilliseconds,
                commitStopwatch.Elapsed.TotalMilliseconds,
                totalStopwatch.Elapsed.TotalMilliseconds);

            return TransactionProcessingOutcome.Processed;
        }
        catch
        {
            await dbTransaction.RollbackAsync(
                cancellationToken);

            throw;
        }
    }
}
