namespace TransactionFlow.Infrastructure.Persistence.Entities;

public sealed class ProcessedTransaction
{
    public required string TransactionId { get; set; }

    public required string MerchantId { get; set; }

    public decimal Amount { get; set; }

    public required string Currency { get; set; }

    public required string Status { get; set; }

    public DateTimeOffset Timestamp { get; set; }

    public DateTimeOffset ProcessedAt { get; set; }
}