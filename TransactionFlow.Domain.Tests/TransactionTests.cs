using TransactionFlow.Domain.Transactions;

namespace TransactionFlow.Domain.Tests;

public class TransactionTests
{
    [Fact]
    public void Should_create_success_transaction()
    {
        var transaction = new Transaction
        {
            TransactionId = "TX-1001",
            MerchantId = "M-001",
            Amount = 125.50m,
            Currency = "EUR",
            Status = TransactionStatus.Success,
            Timestamp = DateTimeOffset.UtcNow
        };

        Assert.Equal("TX-1001", transaction.TransactionId);
        Assert.Equal("M-001", transaction.MerchantId);
        Assert.Equal(125.50m, transaction.Amount);
        Assert.Equal(TransactionStatus.Success, transaction.Status);
    }
}