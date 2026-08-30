namespace TransactionFlow.Application.Transactions;

public sealed class InvalidTransactionException(
    string message)
    : Exception(message);