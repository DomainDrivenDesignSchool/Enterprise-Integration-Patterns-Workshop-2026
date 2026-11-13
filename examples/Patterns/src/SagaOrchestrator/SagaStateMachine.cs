namespace Workshop.SagaOrchestrator;

public enum SagaStatus { Pending, AwaitingPayment, AwaitingInventory, AwaitingShipping, Completed, Compensating, Failed, DeadSaga }
public enum SagaAction { RequestPayment, ReserveInventory, RequestShipment, RefundPayment, ReleaseInventory, Complete, Fail }

public sealed record SagaState(Guid OrderId, SagaStatus Status, string? PaymentReference, string? ReservationId, string? FailureReason, int Version);

public static class SagaStateMachine
{
    public static (SagaState State, SagaAction Action) Start(SagaState s) =>
        Transition(s, SagaStatus.Pending, SagaStatus.AwaitingPayment, SagaAction.RequestPayment);

    public static (SagaState, SagaAction) PaymentAuthorized(SagaState s, string paymentRef) =>
        (s with { Status = SagaStatus.AwaitingInventory, PaymentReference = paymentRef, Version = s.Version + 1 }, SagaAction.ReserveInventory);

    public static (SagaState, SagaAction) PaymentRejected(SagaState s, string reason) =>
        (s with { Status = SagaStatus.Failed, FailureReason = reason, Version = s.Version + 1 }, SagaAction.Fail);

    public static (SagaState, SagaAction) InventoryReserved(SagaState s, string reservationId) =>
        (s with { Status = SagaStatus.AwaitingShipping, ReservationId = reservationId, Version = s.Version + 1 }, SagaAction.RequestShipment);

    public static (SagaState, SagaAction) InventoryRejected(SagaState s, string reason) =>
        (s with { Status = SagaStatus.Compensating, FailureReason = reason, Version = s.Version + 1 },
            s.PaymentReference is null ? SagaAction.Fail : SagaAction.RefundPayment);

    public static (SagaState, SagaAction) ShipmentCreated(SagaState s) =>
        (s with { Status = SagaStatus.Completed, Version = s.Version + 1 }, SagaAction.Complete);

    public static (SagaState, SagaAction) ShipmentFailed(SagaState s, string reason) =>
        (s with { Status = SagaStatus.Compensating, FailureReason = reason, Version = s.Version + 1 },
            s.ReservationId is not null ? SagaAction.ReleaseInventory : SagaAction.RefundPayment);

    private static (SagaState, SagaAction) Transition(SagaState s, SagaStatus expected, SagaStatus next, SagaAction action)
    {
        if (s.Status != expected) throw new InvalidOperationException($"Invalid saga transition {s.Status} -> {next}");
        return (s with { Status = next, Version = s.Version + 1 }, action);
    }
}
