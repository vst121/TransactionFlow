namespace TransactionFlow.Domain.Transactions;

public sealed class TransactionDomainException(
    string message)
    : Exception(message);