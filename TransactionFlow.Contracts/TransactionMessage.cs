namespace TransactionFlow.Contracts;

public sealed record TransactionMessage(
    string TransactionId,
    string MerchantId,
    decimal Amount,
    string Currency,
    string Status,
    DateTimeOffset Timestamp);