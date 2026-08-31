using TransactionFlow.Contracts.Transactions;
using TransactionFlow.Domain.Transactions;

namespace TransactionFlow.Application.Transactions;

public sealed class TransactionProcessingService(
    ITransactionProcessor processor)
    : ITransactionProcessingService
{
    private const int MaxAttempts = 3;

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

        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            try
            {
                return await processor.ProcessAsync(
                    transaction,
                    cancellationToken);
            }
            catch (Exception ex)
            {
                if (!IsTransient(ex))
                {
                    throw;
                }

                if (attempt == MaxAttempts)
                {
                    throw;
                }
            }
        }

        throw new InvalidOperationException(
            "Transaction processing failed unexpectedly.");
    }

    private static bool IsTransient(
        Exception exception)
    {
        return exception is TimeoutException;
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