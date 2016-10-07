using System.Collections.Generic;
using System.Linq;
using NServiceBus;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Json.Linq;

namespace Migrator
{
    internal class TimeoutDataMigrator
    {
        private readonly IDocumentStore _store;

        public TimeoutDataMigrator(IDocumentStore store)
        {
            _store = store;
        }

        public void Migrate(string destinationServerName)
        {
            var allTimeouts = new List<TimeoutData>();
            using (var session = _store.OpenSession())
            {
                var start = 0;
                while (true)
                {
                    var current =
                        session.Query<TimeoutData>()
                            .Take(1024)
                            .Skip(start)
                            .ProjectFromIndexFieldsInto<TimeoutData>()
                            .ToList();
                    if (current.Count == 0)
                        break;

                    start += current.Count;
                    allTimeouts.AddRange(current);
                }
            }
            var updateTimeoutCommands = GetTimeoutDataCommands(allTimeouts, destinationServerName);
            _store.DatabaseCommands.Batch(updateTimeoutCommands);
        }

        private static IEnumerable<ICommandData> GetTimeoutDataCommands(IEnumerable<TimeoutData> allTimeouts, string destinationServerName)
        {
            foreach (var timeoutData in allTimeouts)
            {
                yield return new PatchCommandData()
                {
                    Key = timeoutData.Id,
                    Patches = new[]
                    {
                        new PatchRequest
                        {
                            Type = PatchCommandType.Set,
                            Name = "Destination",
                            Value = RavenJToken.FromObject(new Address(timeoutData.Destination.Queue, destinationServerName))
                        },
                        new PatchRequest
                        {
                            Type = PatchCommandType.Set,
                            Name = "OwningTimeoutManager",
                            Value = timeoutData.OwningTimeoutManager.ReplaceMachineName(destinationServerName)
                        }
                    }
                };
                yield return new ScriptedPatchCommandData
                {
                    Key = timeoutData.Id,
                    Patch = new ScriptedPatchRequest
                    {

                        Script = $"this.Headers[\"NServiceBus.OriginatingMachine\"] = '{destinationServerName}';" +
                                 $"this.Headers[\"NServiceBus.OriginatingEndpoint\"] = '{timeoutData.Headers["NServiceBus.OriginatingEndpoint"].ReplaceMachineName(destinationServerName)}';" +
                                 $"this.Headers[\"NServiceBus.Timeout.RouteExpiredTimeoutTo\"] = '{timeoutData.Headers["NServiceBus.Timeout.RouteExpiredTimeoutTo"].ReplaceMachineName(destinationServerName)}'"
                    }
                };
            }
        }
    }
}