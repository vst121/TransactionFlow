using TransactionFlow.Contracts.Transactions;

namespace TransactionFlow.Application.Transactions;

public interface ITransactionProcessingService
{
    Task<TransactionProcessingOutcome> ProcessAsync(
        TransactionMessage message,
        CancellationToken cancellationToken);
}