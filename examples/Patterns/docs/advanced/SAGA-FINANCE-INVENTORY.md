# A Real Saga for Order, Payment, Inventory and Shipping

## Flow

1. `OrderService` records the order and `OrderCreated` in one SQL Server transaction (Outbox).
2. The Saga receives `OrderCreated` from `commerce.orders`; it stores the Inbox, the Saga state and `RequestPayment` in one local transaction.
3. The Payment Worker uses the PSP with the key `pay:{orderId}`. In this lab `SandboxPaymentGateway` stands in for the PSP, but the idempotency boundary, payment attempt and ledger are real.
4. The payment result is published from the Outbox to `commerce.saga-events`.
5. If the payment is confirmed, the Saga sends the `ReserveInventory` command with the line items.
6. Inventory performs an atomic reservation with `UPDATE stock_items SET available=available-qty WHERE sku=@sku AND available>=qty`. The condition inside the UPDATE itself prevents oversell under concurrent contention.
7. After the reservation, the Saga issues the `RequestShipment` command; the Shipping Worker creates the shipment idempotently with `order_id UNIQUE`.
8. On a shipping failure, the Saga first releases the inventory and, after `InventoryReleased`, refunds the money.

## Transaction boundary

- Order + Outbox: one Order DB transaction.
- Payment attempt/ledger/outbox: one Payment DB transaction; the PSP call is outside the DB transaction and is made safe with the idempotency key.
- Inventory stock/reservation/outbox: one Inventory DB transaction.
- Saga inbox/state/outbox: one Saga DB transaction.
- Each Outbox publisher marks the row Published after broker confirmation. A crash between publish and mark can create a duplicate; the command handler must neutralize the repetition with the idempotency key and unique constraints.

## Financial considerations

This project's Sandbox moves no real money. For a real PSP, the states `Initiated`, `Authorized`, `Captured`, `Rejected`, `RefundPending`, `Refunded`, `Unknown/NeedsReconciliation`, ambiguous timeout, inquiry/reconciliation and callback signatures must be added. On a timeout, before charging again, query the provider's transaction status with the same idempotency key. Keep the Ledger append-only; never edit or delete a previous financial record.

## Inventory considerations

- Negative stock is prohibited with a CHECK constraint and a conditional UPDATE.
- `stock_reservations.order_id UNIQUE` prevents reserving an order again.
- Reservation and release must carry SKU/quantity details and be a single transaction.
- For multiple SKUs, if any one fails, the entire reservation is rolled back; then the `InventoryRejected` event is recorded in a separate transaction.
- Reservation TTL, expiry, returns, backorder and stock adjustment must be designed as independent workflows.
