namespace TransactionFlow.Infrastructure.Persistence.Entities;

public sealed class MerchantAggregate
{
    public required string MerchantId { get; set; }

    public required string Currency { get; set; }

    public long SuccessfulTransactionCount { get; set; }

    public decimal SuccessfulTransactionAmount { get; set; }

    public DateTimeOffset UpdatedAt { get; set; }
}