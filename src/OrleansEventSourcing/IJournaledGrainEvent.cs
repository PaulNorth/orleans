﻿using System;

namespace Orleans.EventSourcing
{
    public interface IJournaledGrainEvent<TGrainState>
        where TGrainState : JournaledGrainState
    {
        void Apply(TGrainState state);
    }

    internal class JournaledGrainSnapshotEvent
    {
        public Int32 LastEventId { get; set; }
        public object State { get; set; }
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
    public class JournaledGrainSnapshotIntervalAttribute : Attribute
    {
        public Int32 Interval { get; set; } = 10;
    }
}
