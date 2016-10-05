using System;
using System.Collections.Generic;
using System.Linq;
using NServiceBus;
using NServiceBus.TimeoutPersisters.RavenDB;
using Raven.Abstractions.Commands;
using Raven.Client.Document;
using Raven.Client.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using JsonExtensions = Raven.Abstractions.Extensions.JsonExtensions;

namespace Migrator
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var ravenUrl = GetRavenUrl();
            Console.Write("Give me your ravendb API key, blank for windows auth");
            var apiKey = Console.ReadLine();
            Console.Write("And the name of your database?");
            var dataBaseName = Console.ReadLine();
            Console.Write("To what machine name should i move the timeouts?");
            var destinationServerName = Console.ReadLine();

            var timeOutMigrator = new TimeOutMigrator();
            var numberOfDocumentsUpdated = timeOutMigrator.Migrate(ravenUrl, apiKey, dataBaseName, destinationServerName);
            Console.Write($"I updated {numberOfDocumentsUpdated} for you");
            Console.Read();
        }

        private static string GetRavenUrl()
        {
            Console.Write("Give me the url of your raven instance: ");
            var ravenUrl = Console.ReadLine();
            while (true)
            {
                try
                {
                    new Uri(ravenUrl);
                    break;
                }
                catch (Exception)
                {
                    Console.Write("Give me the correct and full url(http://...) of your raven instance: ");
                    ravenUrl = Console.ReadLine();
                }

            }
            return ravenUrl;
        }
    }

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
                        var current = session.Query<RavenJObject>().Take(1024).Skip(start).ToList();
                        if (current.Count == 0)
                            break;

                        start += current.Count;
                        current.ForEach(x => allTimeOuts.Add(JsonExtensions.JsonDeserialization<TimeoutData>(x)));
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
                timeoutData.Destination = new Address(timeoutData.Destination.Queue, destinationServerName);
                timeoutData.OwningTimeoutManager = ReplaceMachineName(timeoutData.OwningTimeoutManager,
                    destinationServerName);
                timeoutData.Headers["NServiceBus.Timeout.RouteExpiredTimeoutTo"] =
                    ReplaceMachineName(timeoutData.Headers["NServiceBus.Timeout.RouteExpiredTimeoutTo"],
                        destinationServerName);
                timeoutData.Headers["NServiceBus.OriginatingEndpoint"] = ReplaceMachineName(timeoutData.Headers["NServiceBus.Timeout.RouteExpiredTimeoutTo"],
                        destinationServerName);

                yield return new PutCommandData
                {
                    Key = timeoutData.Id,
                    Document = RavenJObject.FromObject(timeoutData)
                };
            }
        }

        private string ReplaceMachineName(string input, string destinationServerName)
        {
            return input.Substring(0, input.IndexOf("@", StringComparison.Ordinal) + 1) + destinationServerName;
        }
    }
}

namespace NServiceBus.TimeoutPersisters.RavenDB
{
    internal class TimeoutData
    {
        /// <summary>
        /// Id of this timeout
        /// 
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// The address of the client who requested the timeout.
        /// 
        /// </summary>
        public Address Destination { get; set; }

        /// <summary>
        /// The saga ID.
        /// 
        /// </summary>
        public Guid SagaId { get; set; }

        /// <summary>
        /// Additional state.
        /// 
        /// </summary>
        public byte[] State { get; set; }

        /// <summary>
        /// The time at which the timeout expires.
        /// 
        /// </summary>
        public DateTime Time { get; set; }

        /// <summary>
        /// The timeout manager that owns this particular timeout
        /// 
        /// </summary>
        public string OwningTimeoutManager { get; set; }

        /// <summary>
        /// Store the headers to preserve them across timeouts
        /// 
        /// </summary>
        public Dictionary<string, string> Headers { get; set; }

        public TimeoutData()
        {
        }

        public TimeoutData(NServiceBus.Timeout.Core.TimeoutData data)
        {
            this.Destination = data.Destination;
            this.SagaId = data.SagaId;
            this.State = data.State;
            this.Time = data.Time;
            this.OwningTimeoutManager = data.OwningTimeoutManager;
            this.Headers = data.Headers;
        }

        public NServiceBus.Timeout.Core.TimeoutData ToCoreTimeoutData()
        {
            return new NServiceBus.Timeout.Core.TimeoutData()
            {
                Destination = this.Destination,
                Headers = this.Headers,
                OwningTimeoutManager = this.OwningTimeoutManager,
                SagaId = this.SagaId,
                State = this.State,
                Time = this.Time
            };
        }
    }
}
