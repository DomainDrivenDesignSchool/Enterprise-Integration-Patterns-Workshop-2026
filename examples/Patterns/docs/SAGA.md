# Saga and Process Manager

In the extended version, `SagaOrchestrator` is a real Worker that consumes `commerce.orders` and `commerce.saga-events`. `saga_inbox`, `saga_instances` and `saga_outbox` are updated in a single SQL Server transaction. The Outbox publisher sends the command to `commerce.commands` with confirmation.

Main flow:
`OrderCreated → RequestPayment → PaymentAuthorized → ReserveInventory → InventoryReserved → RequestShipment → ShipmentCreated → Completed`

Compensation:
- PaymentRejected: ends as Failed.
- InventoryRejected after payment: RefundPayment.
- ShipmentFailed: ReleaseInventory and then RefundPayment.

Each service ignores commands that are not meant for it; Payment, Inventory and Shipping each have an independent consumer group. Commands are idempotent: payment uses the PSP idempotency key, inventory uses `order_id UNIQUE` and shipment uses `order_id UNIQUE`.

**Important:** In this lab the Payment Worker uses `SandboxPaymentGateway`; the ledger, attempt, idempotency and refund flow are realistic, but no real money is moved. For a real PSP, a dedicated adapter, callback signature validation, timeout ambiguity handling and reconciliation must be added. Also, timeout/deadline handling and human intervention remained as later development in this version.
