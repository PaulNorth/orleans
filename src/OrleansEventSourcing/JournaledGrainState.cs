using System;

namespace Orleans.EventSourcing
{
    internal interface IJournaledGrainState
    {
        object LastEvent { get; set; }
    }

    public abstract class JournaledGrainState : IJournaledGrainState
    {
        Object IJournaledGrainState.LastEvent { get; set; }
    }
}
