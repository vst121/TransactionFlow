using Microsoft.EntityFrameworkCore;
using TransactionFlow.Contracts.Transactions;
using TransactionFlow.Domain.Transactions;

namespace TransactionFlow.Infrastructure.Persistence.Repositories;

public sealed class ProcessedTransactionRepository(
    TransactionFlowDbContext db)
{
    public async Task<bool> TryAddAsync(
        Transaction transaction,
        DateTimeOffset processedAt,
        CancellationToken cancellationToken)
    {
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
            )
            ON CONFLICT (transaction_id)
            DO NOTHING;
            """;

        var affectedRows =
            await db.Database.ExecuteSqlRawAsync(
                sql,
                [
                    transaction.TransactionId,
                    transaction.MerchantId,
                    transaction.Amount,
                    transaction.Currency,
                    transaction.Status,
                    transaction.Timestamp,
                    processedAt
                ],
                cancellationToken);

        return affectedRows == 1;
    }
}