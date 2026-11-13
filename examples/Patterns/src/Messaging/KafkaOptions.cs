namespace Workshop.Messaging;

public sealed class KafkaOptions { public string BootstrapServers {get;set;}="localhost:9092"; public string OrdersTopic {get;set;}="commerce.orders"; public string DlqTopic {get;set;}="commerce.dlq"; }
