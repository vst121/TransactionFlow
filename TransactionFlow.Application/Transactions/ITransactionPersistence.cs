using TransactionFlow.Domain.Transactions;

public interface ITransactionPersistence
{
    Task<ProcessResult> ProcessAsync(
        Transaction transaction,
        CancellationToken cancellationToken);
}
