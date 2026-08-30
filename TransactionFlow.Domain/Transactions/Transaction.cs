namespace TransactionFlow.Domain.Transactions;

public sealed class Transaction
{
    public string TransactionId { get; }

    public string MerchantId { get; }

    public decimal Amount { get; }

    public string Currency { get; }

    public TransactionStatus Status { get; }

    public DateTimeOffset Timestamp { get; }

    private Transaction(
        string transactionId,
        string merchantId,
        decimal amount,
        string currency,
        TransactionStatus status,
        DateTimeOffset timestamp)
    {
        TransactionId = transactionId;
        MerchantId = merchantId;
        Amount = amount;
        Currency = currency;
        Status = status;
        Timestamp = timestamp;
    }

    public static Transaction Create(
        string transactionId,
        string merchantId,
        decimal amount,
        string currency,
        TransactionStatus status,
        DateTimeOffset timestamp)
    {
        if (string.IsNullOrWhiteSpace(transactionId))
            throw new TransactionDomainException(
                "TransactionId is required.");

        if (string.IsNullOrWhiteSpace(merchantId))
            throw new TransactionDomainException(
                "MerchantId is required.");

        if (amount <= 0)
            throw new TransactionDomainException(
                "Amount must be greater than zero.");

        if (string.IsNullOrWhiteSpace(currency))
            throw new TransactionDomainException(
                "Currency is required.");

        if (currency.Length != 3)
            throw new TransactionDomainException(
                "Currency must contain exactly 3 characters.");

        return new Transaction(
            transactionId,
            merchantId,
            amount,
            currency.ToUpperInvariant(),
            status,
            timestamp);
    }
}
