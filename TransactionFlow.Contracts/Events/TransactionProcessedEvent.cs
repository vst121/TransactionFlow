namespace TransactionFlow.Contracts.Events;

public sealed record TransactionProcessedEvent(
    string EventId,
    string TransactionId,
    string MerchantId,
    decimal Amount,
    string Currency,
    DateTimeOffset OccurredAt);
