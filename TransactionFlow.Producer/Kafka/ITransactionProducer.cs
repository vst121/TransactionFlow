using Confluent.Kafka;
using TransactionFlow.Contracts;

namespace TransactionFlow.Producer.Kafka;

public interface ITransactionProducer
{
    Task<DeliveryResult<string, string>> PublishAsync(
        TransactionMessage transaction,
        CancellationToken cancellationToken);
}