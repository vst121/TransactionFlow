namespace TransactionFlow.Contracts.DeadLetter;

public sealed record DeadLetterMessage(
    string OriginalTopic,
    int OriginalPartition,
    long OriginalOffset,
    string ErrorType,
    string ErrorMessage,
    string Payload,
    DateTimeOffset FailedAt);