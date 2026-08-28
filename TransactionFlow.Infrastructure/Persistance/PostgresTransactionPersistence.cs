using Npgsql;
using TransactionFlow.Domain.Transactions;

namespace TransactionFlow.Infrastructure.Persistence;

public sealed class PostgresTransactionPersistence(
    NpgsqlDataSource dataSource)
    : ITransactionPersistence
{
    public async Task<ProcessResult> ProcessAsync(
        Transaction transaction,
        CancellationToken cancellationToken)
    {
        await using var connection =
            await dataSource.OpenConnectionAsync(
                cancellationToken);

        await using var dbTransaction =
            await connection.BeginTransactionAsync(
                cancellationToken);

        try
        {
            // 1. Atomic Dedup
            var inserted =
                await TryInsertProcessedTransactionAsync(
                    connection,
                    dbTransaction,
                    transaction.TransactionId,
                    cancellationToken);

            if (!inserted)
            {
                await dbTransaction.CommitAsync(
                    cancellationToken);

                return ProcessResult.Duplicate;
            }

            // 2. Ignore non-success transactions
            if (transaction.Status != TransactionStatus.Success)
            {
                await dbTransaction.CommitAsync(
                    cancellationToken);

                return ProcessResult.Ignored;
            }

            // 3. Aggregate Upsert
            await UpsertMerchantAggregateAsync(
                connection,
                dbTransaction,
                transaction,
                cancellationToken);

            await dbTransaction.CommitAsync(
                cancellationToken);

            return ProcessResult.Processed;
        }
        catch
        {
            await dbTransaction.RollbackAsync(
                CancellationToken.None);

            throw;
        }
    }

    private static async Task<bool>
        TryInsertProcessedTransactionAsync(
            NpgsqlConnection connection,
            NpgsqlTransaction transaction,
            string transactionId,
            CancellationToken cancellationToken)
    {
        const string sql = """
            INSERT INTO processed_transactions
            (
                transaction_id
            )
            VALUES
            (
                @transaction_id
            )
            ON CONFLICT (transaction_id)
            DO NOTHING
            RETURNING transaction_id;
            """;

        await using var command =
            new NpgsqlCommand(
                sql,
                connection,
                transaction);

        command.Parameters.AddWithValue(
            "transaction_id",
            transactionId);

        var result =
            await command.ExecuteScalarAsync(
                cancellationToken);

        return result is not null;
    }

    private static async Task
        UpsertMerchantAggregateAsync(
            NpgsqlConnection connection,
            NpgsqlTransaction transaction,
            TransactionFlow.Domain.Transactions.Transaction tx,
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
                @merchant_id,
                @currency,
                1,
                @amount,
                @timestamp
            )
            ON CONFLICT (merchant_id, currency)
            DO UPDATE SET
                successful_transaction_count =
                    merchant_aggregates.successful_transaction_count + 1,

                successful_transaction_amount =
                    merchant_aggregates.successful_transaction_amount
                    + EXCLUDED.successful_transaction_amount,

                updated_at =
                    EXCLUDED.updated_at;
            """;

        await using var command =
            new NpgsqlCommand(
                sql,
                connection,
                transaction);

        command.Parameters.AddWithValue(
            "merchant_id",
            tx.MerchantId);

        command.Parameters.AddWithValue(
            "currency",
            tx.Currency.ToUpperInvariant());

        command.Parameters.AddWithValue(
            "amount",
            tx.Amount);

        command.Parameters.AddWithValue(
            "timestamp",
            tx.Timestamp);

        await command.ExecuteNonQueryAsync(
            cancellationToken);
    }
}