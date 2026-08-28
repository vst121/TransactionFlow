using TransactionFlow.Domain.Aggregation;

namespace TransactionFlow.Domain.Tests.Aggregation;

public class MerchantAggregateTests
{
    [Fact]
    public void Should_add_successful_transaction()
    {
        var aggregate =
            new MerchantAggregate("M-001", "EUR");

        aggregate.AddSuccessfulTransaction(
            100m,
            DateTimeOffset.UtcNow);

        Assert.Equal(1, aggregate.SuccessfulTransactionCount);
        Assert.Equal(100m, aggregate.SuccessfulTransactionAmount);
    }

    [Fact]
    public void Should_accumulate_transactions()
    {
        var aggregate =
            new MerchantAggregate("M-001", "EUR");

        aggregate.AddSuccessfulTransaction(
            100m,
            DateTimeOffset.UtcNow);

        aggregate.AddSuccessfulTransaction(
            250.50m,
            DateTimeOffset.UtcNow);

        Assert.Equal(2, aggregate.SuccessfulTransactionCount);
        Assert.Equal(350.50m,
            aggregate.SuccessfulTransactionAmount);
    }

    [Fact]
    public void Should_reject_zero_amount()
    {
        var aggregate =
            new MerchantAggregate("M-001", "EUR");

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            aggregate.AddSuccessfulTransaction(
                0m,
                DateTimeOffset.UtcNow));
    }
}