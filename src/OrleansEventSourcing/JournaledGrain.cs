using System.Threading.Tasks;

namespace Orleans.EventSourcing
{
    /// <summary>
    /// The base class for all grain classes that have event-sourced state.
    /// </summary>
    public abstract class JournaledGrain<TGrainState> : Grain<TGrainState>
        where TGrainState : JournaledGrainState
    {
        protected async Task WriteEvent(IJournaledGrainEvent<TGrainState> @event)
        {
            //set last state?
            ((IJournaledGrainState)this.State).LastEvent = @event;

            await this.WriteStateAsync();
            await ApplyEvent(@event);
        }

        internal Task ApplyEvent(IJournaledGrainEvent<TGrainState> @event)
        {
            @event.Apply(this.State);

            return TaskDone.Done;
        }
    }
}
