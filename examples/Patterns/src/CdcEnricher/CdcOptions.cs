namespace Workshop.CdcEnricher;

public sealed record CdcOptions(string BootstrapServers, string InputTopic, string OutputTopic,
    string GroupId, string TransactionalId, string EnrichmentConnectionString);
