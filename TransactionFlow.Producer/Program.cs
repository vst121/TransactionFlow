using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using TransactionFlow.Producer.Kafka;
using TransactionFlow.Producer.Load;
using TransactionFlow.Producer.Transactions;

var builder = Host.CreateApplicationBuilder(
    new HostApplicationBuilderSettings
    {
        Args = args,
        ContentRootPath = AppContext.BaseDirectory
    });

// Kafka
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

// Load
builder.Services
    .AddOptions<LoadOptions>()
    .Bind(builder.Configuration.GetSection("Load"))
    .Validate(
        options => options.Count > 0,
        "Load Count must be greater than zero.")
    .Validate(
        options => options.Rate > 0,
        "Load Rate must be greater than zero.")
    .ValidateOnStart();

// Transaction Generation
builder.Services
    .AddOptions<TransactionGenerationOptions>()
    .Bind(builder.Configuration.GetSection(
        "TransactionGeneration"))
    .Validate(
        options => options.MerchantCount > 0,
        "MerchantCount must be greater than zero.")
    .Validate(
        options =>
            options.SuccessRate is >= 0 and <= 1,
        "SuccessRate must be between 0 and 1.")
    .Validate(
        options =>
            options.DuplicateRate is >= 0 and <= 1,
        "DuplicateRate must be between 0 and 1.")
    .ValidateOnStart();

// Services

//builder.Services.AddSingleton(sp =>
//    sp.GetRequiredService<
//        IOptions<TransactionGenerationOptions>>().Value);

builder.Services.AddSingleton<
    ITransactionProducer,
    TransactionProducer>();

builder.Services.AddSingleton<TransactionGenerator>();
builder.Services.AddSingleton<TransactionLoadRunner>();

using var host = builder.Build();

var runner =
    host.Services
        .GetRequiredService<TransactionLoadRunner>();

await runner.RunAsync(
    CancellationToken.None);