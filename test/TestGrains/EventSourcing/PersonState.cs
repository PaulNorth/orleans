using Orleans.EventSourcing;
using TestGrainInterfaces;

namespace TestGrains
{
    public class PersonState : JournaledGrainState
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public GenderType Gender { get; set; }
        public bool IsMarried { get; set; }
    }
}
