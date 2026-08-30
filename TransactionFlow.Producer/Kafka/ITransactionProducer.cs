using Confluent.Kafka;
using TransactionFlow.Contracts.Transactions;

namespace TransactionFlow.Producer.Kafka;

public interface ITransactionProducer
{
    Task<DeliveryResult<string, string>> PublishAsync(
        TransactionMessage transaction,
        CancellationToken cancellationToken);
}