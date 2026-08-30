using TransactionFlow.Domain.Transactions;

namespace TransactionFlow.Domain.Tests;

public class TransactionTests
{
    [Fact]
    public void Should_create_success_transaction()
    {
        var transaction = Transaction.Create(
            "TX-1001",
            "M-001",
            125.50m,
            "EUR",
            TransactionStatus.Success,
            DateTimeOffset.UtcNow
            );

        Assert.Equal("TX-1001", transaction.TransactionId);
        Assert.Equal("M-001", transaction.MerchantId);
        Assert.Equal(125.50m, transaction.Amount);
        Assert.Equal(TransactionStatus.Success, transaction.Status);
    }
}