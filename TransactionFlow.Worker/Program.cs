using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using TransactionFlow.Application.Common.Errors;
using TransactionFlow.Application.Outbox;
using TransactionFlow.Application.Transactions;
using TransactionFlow.Infrastructure.Diagnostics;
using TransactionFlow.Infrastructure.Persistence;
using TransactionFlow.Infrastructure.Persistence.Outbox;
using TransactionFlow.Infrastructure.Persistence.Repositories;
using TransactionFlow.Infrastructure.Persistence.Transactions;
using TransactionFlow.Worker;
using TransactionFlow.Worker.Kafka;
using TransactionFlow.Worker.Outbox;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using TransactionFlow.Worker.Telemetry;

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
    .Validate(options =>
        !string.IsNullOrWhiteSpace(options.DeadLetterTopic),
        "Kafka DeadLetterTopic is required.")    
    .ValidateOnStart();


builder.Services.AddSingleton<IProducer<string, string>>(sp =>
{
    var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;

    var config = new ProducerConfig
    {
        BootstrapServers = options.BootstrapServers
    };

    return new ProducerBuilder<string, string>(config).Build();
});

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

builder.Services.AddScoped<
    ITransactionProcessingService,
    TransactionProcessingService>();

builder.Services.AddHostedService<TransactionConsumer>();

builder.Services.AddSingleton<IDeadLetterProducer,
    KafkaDeadLetterProducer>();

builder.Services.AddSingleton<TransactionValidator>();
builder.Services.AddSingleton<IErrorClassifier, ErrorClassifier>();
builder.Services.AddScoped<ITransactionProcessingService,
    TransactionProcessingService>();

builder.Services.AddScoped<
    ISuccessfulTransactionHandler,
    SuccessfulTransactionHandler>();

builder.Services.AddScoped<OutboxMessageRepository>();

builder.Services.AddScoped<IOutboxStore, OutboxStore>();

builder.Services.AddHostedService<OutboxDispatcher>();

builder.Services
    .AddOpenTelemetry()
    .ConfigureResource(resource =>
    {
        resource.AddService(
            serviceName: TransactionFlowTelemetry.ServiceName);
    })
    .WithMetrics(metrics =>
    {
        metrics
            .AddMeter(TransactionFlowTelemetry.ServiceName)
            .AddConsoleExporter();
    })
    .WithTracing(tracing =>
    {
        tracing
            .AddSource(TransactionFlowTelemetry.ServiceName)
            .AddConsoleExporter();
    });

var host = builder.Build();

await host.RunAsync();

//// For test of LatencyProbe
//builder.Services.AddScoped<
//    DatabaseCommitLatencyProbe>();

//using var host = builder.Build();

//using var scope =
//    host.Services.CreateScope();

//var probe =
//    scope.ServiceProvider
//        .GetRequiredService<
//            DatabaseCommitLatencyProbe>();

//await probe.RunAsync(
//    CancellationToken.None);

//return;
