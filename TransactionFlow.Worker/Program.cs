using TransactionFlow.Worker.Kafka;

var builder = Host.CreateApplicationBuilder(args);

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

builder.Services.AddHostedService<TransactionConsumer>();

var host = builder.Build();

await host.RunAsync();