using Workshop.SagaOrchestrator;
using Xunit;
public class SagaTests
{
    [Fact] public void InventoryFailureAfterPayment_RequestsRefund() {
        var state = new SagaState(Guid.NewGuid(), SagaStatus.AwaitingInventory, "pay-1", null, null, 2);
        var (next, action) = SagaStateMachine.InventoryRejected(state, "out of stock");
        Assert.Equal(SagaStatus.Compensating, next.Status);
        Assert.Equal(SagaAction.RefundPayment, action);
    }
}
