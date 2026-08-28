using Microsoft.EntityFrameworkCore;
using TransactionFlow.Infrastructure.Persistence.Entities;

namespace TransactionFlow.Infrastructure.Persistence;

public sealed class TransactionFlowDbContext(
    DbContextOptions<TransactionFlowDbContext> options)
    : DbContext(options)
{
    public DbSet<ProcessedTransaction> ProcessedTransactions
        => Set<ProcessedTransaction>();

    public DbSet<MerchantAggregate> MerchantAggregates
        => Set<MerchantAggregate>();

    protected override void OnModelCreating(
        ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ProcessedTransaction>(entity =>
        {
            entity.HasKey(x => x.TransactionId);

            entity.Property(x => x.Amount)
                .HasPrecision(18, 2);

            entity.HasIndex(x => x.MerchantId);
        });

        modelBuilder.Entity<MerchantAggregate>(entity =>
        {
            entity.HasKey(x => new
            {
                x.MerchantId,
                x.Currency
            });

            entity.Property(x => x.SuccessfulTransactionAmount)
                .HasPrecision(18, 2);
        });
    }
}