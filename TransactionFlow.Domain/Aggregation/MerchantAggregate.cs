namespace TransactionFlow.Domain.Aggregation;

public sealed class MerchantAggregate
{
    public string MerchantId { get; }
    public string Currency { get; }

    public long SuccessfulTransactionCount { get; private set; }

    public decimal SuccessfulTransactionAmount { get; private set; }

    public DateTimeOffset UpdatedAt { get; private set; }

    public MerchantAggregate(
        string merchantId,
        string currency)
    {
        if (string.IsNullOrWhiteSpace(merchantId))
            throw new ArgumentException(
                "Merchant ID is required.",
                nameof(merchantId));

        if (string.IsNullOrWhiteSpace(currency))
            throw new ArgumentException(
                "Currency is required.",
                nameof(currency));

        MerchantId = merchantId;
        Currency = currency.ToUpperInvariant();
    }

    public void AddSuccessfulTransaction(
        decimal amount,
        DateTimeOffset timestamp)
    {
        if (amount <= 0)
            throw new ArgumentOutOfRangeException(nameof(amount));

        SuccessfulTransactionCount++;

        SuccessfulTransactionAmount += amount;

        UpdatedAt = timestamp;
    }
}