using TransactionFlow.Domain.Transactions;

public interface ITransactionPersistence
{
    Task<TransactionProcessingOutcome> ProcessAsync(
        Transaction transaction,
        CancellationToken cancellationToken);
}
