namespace TransactionFlow.Application.Transactions;

public sealed record ValidationResult(
    bool IsValid,
    string? Error)
{
    public static ValidationResult Valid()
        => new(true, null);

    public static ValidationResult Invalid(
        string error)
        => new(false, error);
}
