using Orleans.EventSourcing;
using System;
using TestGrainInterfaces;

namespace TestGrains
{
    public class PersonRegistered : IJournaledGrainEvent<PersonState>
    {
        public string FirstName { get; private set; }
        public string LastName { get; private set; }
        public GenderType Gender { get; private set; }

        public PersonRegistered(string firstName, string lastName, GenderType gender)
        {
            FirstName = firstName;
            LastName = lastName;
            Gender = gender;
        }

        public void Apply(PersonState state)
        {
            state.FirstName = this.FirstName;
            state.LastName = this.LastName;
            state.Gender = this.Gender;
        }
    }

    public class PersonMarried : IJournaledGrainEvent<PersonState>
    {
        public Guid SpouseId { get; private set; }
        public string SpouseFirstName { get; private set; }
        public string SpouseLastName { get; private set; }

        public PersonMarried(Guid spouseId, string spouseFirstName, string spouseLastName)
        {
            SpouseId = spouseId;
            SpouseFirstName = spouseFirstName;
            SpouseLastName = spouseLastName;
        }

        public void Apply(PersonState state)
        {
            state.IsMarried = true;
        }
    }

    public class PersonLastNameChanged : IJournaledGrainEvent<PersonState>
    {
        public string LastName { get; private set; }

        public PersonLastNameChanged(string lastName)
        {
            LastName = lastName;
        }

        public void Apply(PersonState state)
        {
            state.LastName = this.LastName;
        }
    }
}
