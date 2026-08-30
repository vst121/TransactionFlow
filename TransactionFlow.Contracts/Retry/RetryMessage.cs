namespace TransactionFlow.Contracts.Retry;

public sealed record RetryMessage(
    string OriginalTopic,
    int OriginalPartition,
    long OriginalOffset,
    string TransactionId,
    int Attempt,
    string ErrorType,
    string ErrorMessage,
    string Payload,
    DateTimeOffset FailedAt);