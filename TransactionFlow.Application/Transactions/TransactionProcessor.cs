using TransactionFlow.Domain.Transactions;

namespace TransactionFlow.Application.Transactions;

public sealed class TransactionProcessor(
    ISuccessfulTransactionHandler successfulTransactionHandler)
    : ITransactionProcessor
{
    public async Task<TransactionProcessingOutcome> ProcessAsync(
        Transaction transaction,
        CancellationToken cancellationToken)
    {
        if (transaction.Status != TransactionStatus.Success)
        {
            return TransactionProcessingOutcome.Ignored;
        }

        return await successfulTransactionHandler.HandleAsync(
            transaction,
            cancellationToken);
    }
}
