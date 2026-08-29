using Microsoft.EntityFrameworkCore;
using TransactionFlow.Application.Transactions;
using TransactionFlow.Infrastructure.Persistence;
using TransactionFlow.Infrastructure.Persistence.Repositories;
using TransactionFlow.Worker;
using TransactionFlow.Worker.Kafka;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddOptions<FailureInjectionOptions>()
    .Bind(builder.Configuration.GetSection("FailureInjection"));

builder.Services
    .AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(
        KafkaOptions.SectionName))
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.BootstrapServers),
        "Kafka BootstrapServers is required.")
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.Topic),
        "Kafka Topic is required.")
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.GroupId),
        "Kafka GroupId is required.")
    .ValidateOnStart();

builder.Services.AddDbContext<TransactionFlowDbContext>(
    options =>
        options.UseNpgsql(
            builder.Configuration.GetConnectionString(
                "TransactionFlow")));

builder.Services.AddScoped<
    ProcessedTransactionRepository>();

builder.Services.AddScoped<
    MerchantAggregateRepository>();

builder.Services.AddScoped<
    ITransactionProcessor,
    TransactionProcessor>();

builder.Services.AddHostedService<TransactionConsumer>();

var host = builder.Build();

await host.RunAsync();