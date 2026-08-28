namespace TransactionFlow.Domain.Transactions;

public enum TransactionStatus
{
    Pending = 0,
    Success = 1,
    Failed = 2,
    Cancelled = 3
}