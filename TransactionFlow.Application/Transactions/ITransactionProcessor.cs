using TransactionFlow.Contracts;

namespace TransactionFlow.Application.Transactions;

public interface ITransactionProcessor
{
    Task<TransactionProcessingOutcome> ProcessAsync(
        TransactionMessage transaction,
        CancellationToken cancellationToken);
}