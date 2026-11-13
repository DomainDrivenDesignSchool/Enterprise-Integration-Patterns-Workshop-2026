using Workshop.AnalyticsWorker;
var b = Host.CreateApplicationBuilder(args); var c = b.Configuration;

var analyticsConnectionString = c.GetConnectionString("Analytics");

if (string.IsNullOrWhiteSpace(analyticsConnectionString))
    throw new InvalidOperationException("ConnectionStrings:Analytics must be configured for SQL Server.");

b.Services.AddSingleton(new AnalyticsOptions(c["Kafka:BootstrapServers"] ?? "localhost:9092",
    c["Kafka:InputTopic"] ?? "analytics.customer-order-windows.v1",
    c["Kafka:GroupId"] ?? "analytics-window-projection-v1",
    analyticsConnectionString));

b.Services.AddHostedService<Worker>(); await b.Build().RunAsync();
