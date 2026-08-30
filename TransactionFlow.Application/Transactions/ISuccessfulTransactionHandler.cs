using TransactionFlow.Domain.Transactions;

namespace TransactionFlow.Application.Transactions;

public interface ISuccessfulTransactionHandler
{
    Task<TransactionProcessingOutcome> HandleAsync(
        Transaction transaction,
        CancellationToken cancellationToken);
}
