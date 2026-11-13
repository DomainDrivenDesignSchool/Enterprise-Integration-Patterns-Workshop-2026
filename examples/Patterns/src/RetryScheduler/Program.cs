using Workshop.RetryScheduler;

var b = Host.CreateApplicationBuilder(args);
var c = b.Configuration;

var retryConnectionString = c.GetConnectionString("RetrySchedule");
if (string.IsNullOrWhiteSpace(retryConnectionString))
    throw new InvalidOperationException("ConnectionStrings:RetrySchedule must be configured for SQL Server.");

b.Services.AddSingleton(new RetryOptions(c["Kafka:BootstrapServers"]
    ?? "localhost:9092",
    c["Kafka:RetryTopics"]
    ?? "commerce.retry.1m,commerce.retry.5m,commerce.retry.15m,commerce.retry.1h",
    c["Kafka:GroupId"]
    ?? "retry-scheduler-v1",
    retryConnectionString));

b.Services.AddHostedService<Worker>(); await b.Build().RunAsync();
