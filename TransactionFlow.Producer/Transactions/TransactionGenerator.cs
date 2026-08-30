using Microsoft.Extensions.Options;
using TransactionFlow.Contracts.Transactions;

namespace TransactionFlow.Producer.Transactions;

public sealed class TransactionGenerator
{
    private readonly TransactionGenerationOptions _options;
    private readonly string[] _merchants;

    public TransactionGenerator(
        IOptions<TransactionGenerationOptions> options)
    {
        _options = options.Value;
    
        _merchants =
            Enumerable
                .Range(1, _options.MerchantCount)
                .Select(i => $"M-{i:000}")
                .ToArray();
    }

    private readonly string[] _currencies =
    [
        "EUR",
        "USD"
    ];

    public TransactionMessage Generate()
    {
        var merchant =
            _merchants[
                Random.Shared.Next(_merchants.Length)];

        var currency =
            _currencies[
                Random.Shared.Next(_currencies.Length)];

        var status =
            Random.Shared.NextDouble()
                < _options.SuccessRate
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