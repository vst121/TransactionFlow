using Microsoft.EntityFrameworkCore;
using Npgsql;
using TransactionFlow.Infrastructure.Persistence.Entities;

namespace TransactionFlow.Infrastructure.Persistence.Repositories;

public sealed class MerchantAggregateRepository(
    TransactionFlowDbContext db)
{
    public async Task UpsertSuccessfulAsync(
        string merchantId,
        string currency,
        decimal amount,
        DateTimeOffset updatedAt,
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
                {3}
            )
            ON CONFLICT (merchant_id, currency)
            DO UPDATE SET
                successful_transaction_count =
                    merchant_aggregates.successful_transaction_count + 1,

                successful_transaction_amount =
                    merchant_aggregates.successful_transaction_amount
                    + EXCLUDED.successful_transaction_amount,

                updated_at = EXCLUDED.updated_at;
            """;

        await db.Database.ExecuteSqlRawAsync(
            sql,
            new object[] { merchantId, currency, amount, updatedAt },
            cancellationToken);
    }
}