using Workshop.ShippingWorker;
var builder = Host.CreateApplicationBuilder(args);

var c = builder.Configuration;

var shippingConnectionString = c.GetConnectionString("Shipping");

if (string.IsNullOrWhiteSpace(shippingConnectionString))
    throw new InvalidOperationException("ConnectionStrings:Shipping must be configured for SQL Server.");

builder.Services.AddSingleton(new ShippingOptions(c["Kafka:BootstrapServers"] ?? "localhost:9092", c["Kafka:CommandTopic"] ?? "commerce.commands", c["Kafka:SagaEventsTopic"] ?? "commerce.saga-events", c["Kafka:GroupId"] ?? "shipping-command-v1", shippingConnectionString));

builder.Services.AddHostedService<Worker>();
await builder.Build().RunAsync();
