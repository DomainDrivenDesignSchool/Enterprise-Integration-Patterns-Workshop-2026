using Microsoft.AspNetCore.Builder;
using Confluent.Kafka;
using Workshop.SagaOrchestrator;

var builder = WebApplication.CreateBuilder(args);

var cfg = builder.Configuration;

var sagaConnectionString = cfg.GetConnectionString("Saga");

if (string.IsNullOrWhiteSpace(sagaConnectionString)) 
    throw new InvalidOperationException("ConnectionStrings:Saga must be configured for SQL Server.");

var options = new SagaOptions(
    cfg["Kafka:BootstrapServers"] ?? "localhost:9092",
    cfg["Kafka:InputTopics"] ?? "commerce.orders,commerce.saga-events",
    cfg["Kafka:CommandTopic"] ?? "commerce.commands",
    cfg["Kafka:SagaEventsTopic"] ?? "commerce.saga-events",
    cfg["Kafka:GroupId"] ?? "order-saga-v1",
    sagaConnectionString);

builder.Services.AddSingleton(options);

builder.Services.AddSingleton(new SagaOperationsOptions(cfg["Operations:ApiKey"] ?? string.Empty));
builder.Services.AddHostedService<SagaWorker>();

builder.Services.AddHostedService<SagaRecoveryWorker>();

var app = builder.Build();


app.MapSagaOperations();
await app.RunAsync();
