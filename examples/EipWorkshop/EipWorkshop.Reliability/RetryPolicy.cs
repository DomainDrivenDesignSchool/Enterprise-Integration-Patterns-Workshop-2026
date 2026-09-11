using EipWorkshop.Core;

namespace EipWorkshop.Reliability;

public sealed class RetryPolicy
{
    public int MaxAttempts { get; init; } = 3;
    public TimeSpan BaseDelay { get; init; } = TimeSpan.FromMilliseconds(100);


    // pipe
    // retry pipe
    // command handler pipe
    //
    // controller => action
    // mediatr => dispatch => command 

    public async Task ExecuteAsync<T>(Message<T> msg, Func<Message<T>, Task> action,
        Action<Message<T>, Exception>? onDeadLetter = null)
    {
        for (int attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            // circute breaker

            msg.DeliveryAttempt = attempt;
            try { await action(msg); return; }
            catch (Exception ex) when (attempt < MaxAttempts)
            {
                var delay = TimeSpan.FromMilliseconds(BaseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1));
                await Task.Delay(delay);
            }
            catch (Exception ex)
            {
                onDeadLetter?.Invoke(msg, ex);
                return;
            }
        }
    }
}