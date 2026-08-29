using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TransactionFlow.Producer.Kafka;
using TransactionFlow.Producer.Transactions;

var builder = Host.CreateApplicationBuilder(
    new HostApplicationBuilderSettings
    {
        Args = args,
        ContentRootPath = AppContext.BaseDirectory
    });

//Console.WriteLine($"CurrentDirectory: {Directory.GetCurrentDirectory()}");
//Console.WriteLine($"AppContext.BaseDirectory: {AppContext.BaseDirectory}");
//Console.WriteLine(
//    $"Kafka BootstrapServers: " +
//    $"{builder.Configuration["Kafka:BootstrapServers"]}");
//Console.WriteLine(
//    $"Kafka Topic: " +
//    $"{builder.Configuration["Kafka:Topic"]}");
//Console.WriteLine($"Environment: {builder.Environment.EnvironmentName}");
//Console.WriteLine($"ContentRoot: {builder.Environment.ContentRootPath}");
//Console.WriteLine($"BaseDirectory: {AppContext.BaseDirectory}");
//Console.WriteLine(
//    $"Kafka BootstrapServers: " +
//    $"{builder.Configuration["Kafka:BootstrapServers"]}");
//Console.WriteLine(
//    $"Kafka Topic: " +
//    $"{builder.Configuration["Kafka:Topic"]}");
//Console.WriteLine();
//Console.WriteLine("Configuration providers:");
//foreach (var provider in builder.Configuration.Sources)
//{
//    Console.WriteLine(provider.GetType().FullName);
//}

builder.Services
    .AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(
        KafkaOptions.SectionName))
    .Validate(
        options =>
            !string.IsNullOrWhiteSpace(
                options.BootstrapServers),
        "BootstrapServers is required.")
    .Validate(
        options =>
            !string.IsNullOrWhiteSpace(
                options.Topic),
        "Topic is required.")
    .ValidateOnStart();

builder.Services.AddSingleton<
    ITransactionProducer,
    TransactionProducer>();

builder.Services.AddSingleton<
    TransactionGenerator>();

using var host = builder.Build();

var producer =
    host.Services
        .GetRequiredService<ITransactionProducer>();

var generator =
    host.Services
        .GetRequiredService<TransactionGenerator>();

for (var i = 0; i < 10; i++)
{
    var transaction = generator.Generate();

    var result =
        await producer.PublishAsync(
            transaction,
            CancellationToken.None);

    Console.WriteLine(
        $"Transaction={transaction.TransactionId} " +
        $"Merchant={transaction.MerchantId} " +
        $"Amount={transaction.Amount} " +
        $"Status={transaction.Status} " +
        $"Partition={result.Partition} " +
        $"Offset={result.Offset}");
}