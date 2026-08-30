using TransactionFlow.Contracts.Transactions;
using TransactionFlow.Domain.Transactions;

namespace TransactionFlow.Application.Transactions;

public sealed class TransactionProcessingService(
    ITransactionProcessor processor)
    : ITransactionProcessingService
{
    public async Task<TransactionProcessingOutcome> ProcessAsync(
        TransactionMessage message,
        CancellationToken cancellationToken)
    {
        var transaction =
            Transaction.Create(
                transactionId: message.TransactionId,
                merchantId: message.MerchantId,
                amount: message.Amount,
                currency: message.Currency,
                status: ParseStatus(message.Status),
                timestamp: message.Timestamp);

        return await processor.ProcessAsync(
            transaction,
            cancellationToken);
    }

    private static TransactionStatus ParseStatus(
        string status)
    {
        if (Enum.TryParse<TransactionStatus>(
                status,
                ignoreCase: true,
                out var result))
        {
            return result;
        }

        throw new TransactionDomainException(
            $"Unsupported transaction status: '{status}'.");
    }
}
