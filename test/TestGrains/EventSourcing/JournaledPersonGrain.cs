using Orleans;
using Orleans.EventSourcing;
using Orleans.Providers;
using System;
using System.Threading.Tasks;
using TestGrainInterfaces;

namespace TestGrains
{
    [StorageProvider(ProviderName = "EventStoreStorageProvider")]
    public class JournaledPersonGrain : JournaledGrain<PersonState>, IJournaledPersonGrain
    {
        public Task RegisterBirth(PersonAttributes props)
        {
            return WriteEvent(new PersonRegistered(props.FirstName, props.LastName, props.Gender));
        }

        public async Task Marry(IJournaledPersonGrain spouse)
        {
            if (State.IsMarried)
                throw new NotSupportedException(string.Format("{0} is already married.", State.LastName));

            var spouseData = await spouse.GetPersonalAttributes();

            await WriteEvent(
                new PersonMarried(spouse.GetPrimaryKey(), spouseData.FirstName, spouseData.LastName)); // We are not storing the first event here

            if (State.LastName != spouseData.LastName)
            {
                await WriteEvent(
                    new PersonLastNameChanged(spouseData.LastName));
            }
        }

        public Task<PersonAttributes> GetPersonalAttributes()
        {
            return Task.FromResult(new PersonAttributes
            {
                FirstName = State.FirstName,
                LastName = State.LastName,
                Gender = State.Gender
            });
        }
    }
}
