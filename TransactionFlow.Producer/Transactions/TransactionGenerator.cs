using TransactionFlow.Contracts;

namespace TransactionFlow.Producer.Transactions;

public sealed class TransactionGenerator
{
    private static readonly string[] Merchants =
    [
        "M-001",
        "M-002",
        "M-003",
        "M-004",
        "M-005"
    ];

    private static readonly string[] Currencies =
    [
        "EUR",
        "USD"
    ];

    public TransactionMessage Generate()
    {
        var merchant =
            Merchants[
                Random.Shared.Next(Merchants.Length)];

        var currency =
            Currencies[
                Random.Shared.Next(Currencies.Length)];

        var status =
            Random.Shared.NextDouble() < 0.9
                ? "SUCCESS"
                : "FAILED";

        var amount =
            Math.Round(
                Random.Shared.NextDouble() * 999 + 1,
                2);

        return new TransactionMessage(
            TransactionId: $"TX-{Guid.NewGuid():N}",
            MerchantId: merchant,
            Amount: (decimal)amount,
            Currency: currency,
            Status: status,
            Timestamp: DateTimeOffset.UtcNow);
    }
}