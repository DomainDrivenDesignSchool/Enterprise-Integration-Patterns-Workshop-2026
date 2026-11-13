using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Workshop.Contracts;
using Workshop.Messaging;
using Workshop.Persistence;

var b = WebApplication.CreateBuilder(args);
var cs = b.Configuration.GetConnectionString("Orders");

if (string.IsNullOrWhiteSpace(cs))
    throw new InvalidOperationException("ConnectionStrings:Orders must be configured for SQL Server.");

var ko = b.Configuration.GetSection("Kafka").Get<KafkaOptions>()
    ??
    new();

b.Services.AddDbContext<OrderDb>(o => o.UseSqlServer(cs));
b.Services.AddSingleton(ko); b.Services.AddSingleton<IEventPublisher, KafkaPublisher>(); b.Services.AddHostedService<OutboxPublisher>(); b.Services.AddHostedService<OrderLifecycleWorker>();
b.Services.AddOpenTelemetry().ConfigureResource(r => r.AddService("order-service")).WithTracing(t => t.AddAspNetCoreInstrumentation().AddHttpClientInstrumentation().AddOtlpExporter());
b.Services.AddOpenApi();

var app = b.Build(); app.MapOpenApi();


app.MapPost("/orders", async (CreateOrderRequest req, OrderDb db, CancellationToken ct) =>
{
    if (req.CustomerId == Guid.Empty || req.Lines is null || req.Lines.Count == 0)
        return Results.BadRequest("CustomerId and at least one order line are required.");

    if (string.IsNullOrWhiteSpace(req.Currency)
    || req.Currency.Length != 3
    || req.Currency.Any(ch => !char.IsAsciiLetterUpper(ch)))
        return Results.BadRequest("Currency must be a 3-letter uppercase ISO-style code.");

    if (req.Lines.Any(x => string.IsNullOrWhiteSpace(x.Sku) || x.Quantity <= 0
    || x.UnitPrice <= 0))
        return Results.BadRequest("Each line needs a SKU, positive quantity and positive unit price.");

    var calculatedTotal = req.Lines.Sum(x => x.Quantity * x.UnitPrice);

    if (req.TotalAmount > 0 && req.TotalAmount != calculatedTotal)
        return Results.BadRequest("TotalAmount must equal sum(quantity * unitPrice).");

    var id = Guid.NewGuid(); var now = DateTimeOffset.UtcNow; var correlation = req.CorrelationId == Guid.Empty ? Guid.NewGuid() : req.CorrelationId;

    var evt = new IntegrationEvent<OrderCreated>(Guid.NewGuid(), EventTypes.OrderCreated, 1, now, "order-service", correlation, null, new(id, req.CustomerId, calculatedTotal, req.Currency, req.Lines.Select(x => new OrderLine(x.Sku, x.Quantity, x.UnitPrice)).ToArray()));

    await using var tx = await db.Database.BeginTransactionAsync(ct);

    db.Orders.Add(new Order
    {
        Id = id,
        CustomerId = req.CustomerId,
        TotalAmount = calculatedTotal,
        Currency = req.Currency,
        CreatedAt = now,
        Items = req.Lines.Select(x => new OrderItem { Id = Guid.NewGuid(), Sku = x.Sku, Quantity = x.Quantity, UnitPrice = x.UnitPrice }).ToList()
    });

    db.Outbox.Add(new OutboxMessage
    {
        Id = evt.EventId,
        AggregateId = id,
        EventType = evt.EventType,
        Payload = JsonSerializer.Serialize(evt),
        OccurredAt = now,
        NextAttemptAt = now
    });

    await db.SaveChangesAsync(ct); await tx.CommitAsync(ct);
    return Results.Accepted($"/orders/{id}", new { id, eventId = evt.EventId, correlationId = correlation, status = "Pending" });
});


app.MapGet("/orders/{id:guid}", async (Guid id, OrderDb db, CancellationToken ct) => await db.Orders.FindAsync([id], ct) is { } x ? Results.Ok(x) : Results.NotFound());

app.MapGet("/health/live", () => Results.Ok(new { status = "live" }));

app.Run();
