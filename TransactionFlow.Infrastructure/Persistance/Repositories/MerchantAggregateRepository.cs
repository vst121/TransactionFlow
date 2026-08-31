using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using TransactionFlow.Domain.Transactions;

namespace TransactionFlow.Infrastructure.Persistence.Repositories;

public sealed class MerchantAggregateRepository(
    TransactionFlowDbContext db,
    ILogger<MerchantAggregateRepository> logger)
{
    public async Task UpsertAsync(
        Transaction transaction,
        CancellationToken cancellationToken)
    {
        const string sql = """
            INSERT INTO merchant_aggregates
            (
                merchant_id,
                currency,
                successful_transaction_count,
                successful_transaction_amount,
                updated_at
            )
            VALUES
            (
                {0},
                {1},
                1,
                {2},
                NOW()
            )
            ON CONFLICT (merchant_id, currency)
            DO UPDATE SET
                successful_transaction_count =
                    merchant_aggregates.successful_transaction_count + 1,

                successful_transaction_amount =
                    merchant_aggregates.successful_transaction_amount
                    + EXCLUDED.successful_transaction_amount,

                updated_at = NOW();
            """;

        await db.Database.ExecuteSqlRawAsync(
            $"SET synchronous_commit = off;",
            cancellationToken);

        var stopwatch = Stopwatch.StartNew();

        await db.Database.ExecuteSqlRawAsync(
            sql,
            [
                transaction.MerchantId,
                transaction.Currency,
                transaction.Amount
            ],
            cancellationToken);

        stopwatch.Stop();

        logger.LogDebug(
            "Merchant aggregate upsert completed. " +
            "TransactionId={TransactionId}, " +
            "MerchantId={MerchantId}, " +
            "Currency={Currency}, " +
            "DurationMs={DurationMs:F2}",
            transaction.TransactionId,
            transaction.MerchantId,
            transaction.Currency,
            stopwatch.Elapsed.TotalMilliseconds);
    }
}
