using System;
using System.Collections.Generic;
using System.Linq;
using NServiceBus;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Migrator
{
    internal class TimeOutMigrator
    {
        public int Migrate(string ravenUrl, string apiKey, string dataBaseName, string destinationServerName)
        {
            using (var store = new DocumentStore
            {
                Url = ravenUrl,
                DefaultDatabase = dataBaseName,
                ApiKey = apiKey
            }.Initialize())
            {
                var allTimeOuts = new List<TimeoutData>();
                using (var session = store.OpenSession())
                {
                    var start = 0;
                    while (true)
                    {
                        var current = session.Query<TimeoutData>().Take(1024).Skip(start).ProjectFromIndexFieldsInto<TimeoutData>().ToList();
                        if (current.Count == 0)
                            break;

                        start += current.Count;
                        allTimeOuts.AddRange(current);
                    }
                }
                var updateTimeOutCommands = GetTimeOutDataCommands(allTimeOuts, destinationServerName);
                var batchResults = store.DatabaseCommands.Batch(updateTimeOutCommands);
                return batchResults.Length;
            }
        }

        private IEnumerable<ICommandData> GetTimeOutDataCommands(List<TimeoutData> allTimeOuts, string destinationServerName)
        {
            foreach (var timeoutData in allTimeOuts)
            {
                yield return new PatchCommandData()
                {
                    Key = timeoutData.Id,
                    Patches = new []
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
                            Value = ReplaceMachineName(timeoutData.OwningTimeoutManager, destinationServerName)
                        }
                    }
                };
                yield return new ScriptedPatchCommandData
                {
                    Key = timeoutData.Id,
                    Patch = new ScriptedPatchRequest
                    {
                        
                        Script = $"this.Headers[\"NServiceBus.OriginatingMachine\"] = '{destinationServerName}';" +
                                 $"this.Headers[\"NServiceBus.OriginatingEndpoint\"] = '{ReplaceMachineName(timeoutData.Headers["NServiceBus.OriginatingEndpoint"], destinationServerName)}';" +
                                 $"this.Headers[\"NServiceBus.Timeout.RouteExpiredTimeoutTo\"] = '{ReplaceMachineName(timeoutData.Headers["NServiceBus.Timeout.RouteExpiredTimeoutTo"], destinationServerName)}'"
                    }
                };
            }
        }

        private string ReplaceMachineName(string input, string destinationServerName)
        {
            return input.Substring(0, input.IndexOf("@", StringComparison.Ordinal) + 1) + destinationServerName;
        }
    }
}