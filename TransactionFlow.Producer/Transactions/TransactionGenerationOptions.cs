namespace TransactionFlow.Producer.Transactions;

public sealed class TransactionGenerationOptions
{
    public int MerchantCount { get; init; } = 10;

    public double SuccessRate { get; init; } = 0.90;

    public double DuplicateRate { get; init; } = 0.00;
}