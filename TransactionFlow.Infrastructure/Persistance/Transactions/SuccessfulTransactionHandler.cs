using Microsoft.EntityFrameworkCore.Design;
using Npgsql;
using Npgsql.EntityFrameworkCore.PostgreSQL;
using Npgsql.EntityFrameworkCore;
using TransactionFlow.Application.Transactions;
using TransactionFlow.Domain.Transactions;
using TransactionFlow.Infrastructure.Persistence.Repositories;

namespace TransactionFlow.Infrastructure.Persistence.Transactions;

public sealed class SuccessfulTransactionHandler(
    TransactionFlowDbContext db,
    ProcessedTransactionRepository processedTransactions,
    MerchantAggregateRepository merchantAggregates)
    : ISuccessfulTransactionHandler
{
    public async Task<TransactionProcessingOutcome> HandleAsync(
        Transaction transaction,
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

            // Use the existing repository method and pass transaction properties
            await merchantAggregates.UpsertSuccessfulAsync(
                transaction.MerchantId,
                transaction.Currency,
                transaction.Amount,
                transaction.Timestamp,
                cancellationToken);

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
