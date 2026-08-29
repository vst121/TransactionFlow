namespace TransactionFlow.Worker;

public sealed class FailureInjectionOptions
{
    public bool CrashAfterDatabaseCommit { get; set; }
}