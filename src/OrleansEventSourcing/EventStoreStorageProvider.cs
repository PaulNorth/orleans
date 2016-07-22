using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;
using EventStore.ClientAPI.SystemData;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Orleans.Runtime;
using Orleans.Storage;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.EventSourcing
{
    public class EventStoreStorageProvider : IStorageProvider
    {
        private IEventStoreConnection Connection;

        private const int ReadPageSize = 5;
        private const int SnapshotInterval = 10;

        private static readonly JsonSerializerSettings SerializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None };

        public string Name { get; private set; }
        public Logger Log { get; private set; }

        public async Task Init(string name, Orleans.Providers.IProviderRuntime providerRuntime, Orleans.Providers.IProviderConfiguration config)
        {
            this.Name = name;
            this.Log = providerRuntime.GetLogger(this.GetType().FullName);

            // Create EventStore connection
            var username = config.Properties.ContainsKey("Username") ? config.Properties["Username"] : "admin";
            var password = config.Properties.ContainsKey("Password") ? config.Properties["Password"] : "changeit";

            var settings = ConnectionSettings.Create()
                //.KeepReconnecting().KeepRetrying()
                .SetDefaultUserCredentials(new UserCredentials(username, password))
                .FailOnNoServerResponse()
                .SetHeartbeatTimeout(TimeSpan.FromMinutes(1))
                .UseConsoleLogger();

            // Connection string format: <hostName>:<port>
            var connectionString = new Uri(config.Properties["ConnectionString"]);
            var hostName = connectionString.Host;
            var hostPort = connectionString.Port;
            var hostAddress = Dns.GetHostAddresses(hostName).First(a => a.AddressFamily == AddressFamily.InterNetwork);

            this.Connection = EventStoreConnection.Create(settings, new IPEndPoint(hostAddress, hostPort));

            // Connect to EventStore
            await this.Connection.ConnectAsync();
        }

        public Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            return TaskDone.Done;
        }

        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var snapshotStream = this.GetSnapshotStreamName(grainType, grainReference);
            Type stateType = grainState.GetType().GetGenericArguments()[0];

            StreamEventsSlice latestSnapshot;
            latestSnapshot = await this.Connection.ReadStreamEventsBackwardAsync(snapshotStream, StreamPosition.End, 1, false);

            if (latestSnapshot.Events.Any())
            {
                var snapshot = JsonConvert.DeserializeObject<JournaledGrainSnapshotEvent>(Encoding.UTF8.GetString(latestSnapshot.Events.First().Event.Data));
                JsonConvert.PopulateObject(snapshot.State.ToString(), grainState.State);
                grainState.ETag = snapshot.LastEventId.ToString();
            }

            var stream = this.GetStreamName(grainType, grainReference);

            var sliceStart = grainState.ETag == null ? 0 : Convert.ToInt32(grainState.ETag);
            StreamEventsSlice currentSlice;

            dynamic state = Convert.ChangeType(grainState.State, stateType);

            do
            {
                var sliceCount = sliceStart + ReadPageSize;

                currentSlice = await this.Connection.ReadStreamEventsForwardAsync(stream, sliceStart, sliceCount, false);

                if (currentSlice.Status == SliceReadStatus.StreamNotFound)
                    return;

                if (currentSlice.Status == SliceReadStatus.StreamDeleted)
                    throw new StreamDeletedException();

                sliceStart = currentSlice.NextEventNumber;

                foreach (var @event in currentSlice.Events)
                {
                    dynamic deserialisedEvent = DeserializeEvent(@event.Event);
                    deserialisedEvent.Apply(state);
                }

            } while (!currentSlice.IsEndOfStream);

            grainState.ETag = currentSlice.LastEventNumber.ToString();
        }

        public async Task WriteStateAsync(string grainType, GrainReference grainReference, Orleans.IGrainState grainState)
        {
            var stream = this.GetStreamName(grainType, grainReference);

            var newEvent = ((IJournaledGrainState)grainState.State).LastEvent;
            if (newEvent == null) return;

            //Write the Event
            var expectedVersion = grainState.ETag == null ? ExpectedVersion.NoStream : Convert.ToInt32(grainState.ETag);
            var eventToSave = ToEventData(newEvent);
            var writeResult = await this.Connection.AppendToStreamAsync(stream, expectedVersion, new EventData[] { eventToSave });
            ((IJournaledGrainState)grainState.State).LastEvent = null;

            //If the SnapshotInterval has been reached...
            if (SnapshotInterval != 0 && grainState.ETag != null && Convert.ToInt32(grainState.ETag) % SnapshotInterval == 0)
            {
                var snapshotStream = this.GetSnapshotStreamName(grainType, grainReference);
                var snapshotToSave = ToEventData(new JournaledGrainSnapshotEvent { LastEventId = Convert.ToInt32(grainState.ETag), State = grainState.State });
                await this.Connection.AppendToStreamAsync(snapshotStream, ExpectedVersion.Any, new EventData[] { snapshotToSave });
            }

            grainState.ETag = writeResult.NextExpectedVersion.ToString();
        }

        public Task Close()
        {
            this.Connection.Close();

            return TaskDone.Done;
        }

        private String GetGrainKey(GrainReference grainReference)
        {
            //Assume default KeyType == Guid  (Note: Integer KeyTypes return an encoded Guid)
            String extendKey;
            Guid key = grainReference.GetPrimaryKey(out extendKey);

            if (key != Guid.Empty) return key.ToString();
            return extendKey;
        }

        private String GetStreamName(string grainType, GrainReference grainReference)
        {
            return $"{grainType}::{GetGrainKey(grainReference)}";
        }

        private string GetSnapshotStreamName(string grainType, GrainReference grainReference)
        {
            return $"{GetStreamName(grainType, grainReference)}::Snapshots";
        }

        #region Event serialisation

        private static object DeserializeEvent(RecordedEvent @event)
        {
            var eventType = Type.GetType(@event.EventType);
            Debug.Assert(eventType != null, "Couldn't load type '{0}'. Are you missing an assembly reference?", @event.EventType);

            return JsonConvert.DeserializeObject(Encoding.UTF8.GetString(@event.Data), eventType);
        }

        private static JObject DeserializeMetadata(byte[] metadata)
        {
            return JObject.Parse(Encoding.UTF8.GetString(metadata));
        }

        private static EventData ToEventData(object processedEvent)
        {
            return ToEventData(Guid.NewGuid(), processedEvent, new Dictionary<string, object>());
        }

        private static EventData ToEventData(Guid eventId, object evnt, IDictionary<string, object> headers)
        {
            var data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(evnt, SerializerSettings));
            var metadata = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(headers, SerializerSettings));

            var eventTypeName = evnt.GetType().AssemblyQualifiedName;
            return new EventData(eventId, eventTypeName, true, data, metadata);
        }

        #endregion
    }
}