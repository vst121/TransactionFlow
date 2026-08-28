using TransactionFlow.Application.Transactions;
using TransactionFlow.Contracts;
using TransactionFlow.Infrastructure.Persistence.Repositories;

namespace TransactionFlow.Infrastructure.Persistence;

public sealed class TransactionProcessor(
    TransactionFlowDbContext db,
    ProcessedTransactionRepository processedTransactions,
    MerchantAggregateRepository merchantAggregates)
    : ITransactionProcessor
{
    public async Task<TransactionProcessingOutcome> ProcessAsync(
        TransactionMessage transaction,
        CancellationToken cancellationToken)
    {
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

                return TransactionProcessingOutcome.Duplicate;
            }

            if (transaction.Status.Equals(
                    "SUCCESS",
                    StringComparison.OrdinalIgnoreCase))
            {
                await merchantAggregates.UpsertSuccessfulAsync(
                    transaction.MerchantId,
                    transaction.Currency,
                    transaction.Amount,
                    DateTimeOffset.UtcNow,
                    cancellationToken);
            }

            await dbTransaction.CommitAsync(
                cancellationToken);

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