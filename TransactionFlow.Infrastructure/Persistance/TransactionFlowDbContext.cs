using Microsoft.EntityFrameworkCore;
using TransactionFlow.Infrastructure.Persistence.Entities;
using TransactionFlow.Infrastructure.Persistence.Outbox;

namespace TransactionFlow.Infrastructure.Persistence;

public sealed class TransactionFlowDbContext(
    DbContextOptions<TransactionFlowDbContext> options)
    : DbContext(options)
{
    public DbSet<ProcessedTransaction> ProcessedTransactions
        => Set<ProcessedTransaction>();

    public DbSet<MerchantAggregate> MerchantAggregates
        => Set<MerchantAggregate>();

    public DbSet<OutboxMessage> OutboxMessages =>
        Set<OutboxMessage>();

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

        modelBuilder.Entity<OutboxMessage>(entity =>
        {
            entity.ToTable("outbox_messages");

            entity.HasKey(x => x.Id);

            entity.Property(x => x.Type)
                .IsRequired();

            entity.Property(x => x.Payload)
                .IsRequired();

            entity.Property(x => x.OccurredAt)
                .IsRequired();

            entity.Property(x => x.PublishedAt);

            entity.Property(x => x.Attempts)
                .IsRequired();

            entity.HasIndex(x => new
            {
                x.PublishedAt,
                x.OccurredAt
            });
        });
    }
}