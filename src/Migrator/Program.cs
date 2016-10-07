using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Json.Linq;

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

            using (var store = new DocumentStore
            {
                Url = ravenUrl,
                DefaultDatabase = dataBaseName,
                ApiKey = apiKey
            }.Initialize())
            {
                var timeoutDataMigrator = new TimeoutDataMigrator(store);
                timeoutDataMigrator.Migrate(destinationServerName);
                var sagaDataMigrator = new SagaDataMigrator(store);
                sagaDataMigrator.Migrate(destinationServerName);
            }
            Console.Write($"I am done");
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

    internal class SagaDataMigrator
    {
        private readonly IDocumentStore _store;

        public SagaDataMigrator(IDocumentStore store)
        {
            _store = store;
        }

        public void Migrate(string destinationServerName)
        {
            var patchCommands = new List<PatchCommandData>();
            using (var session = _store.OpenSession())
            {
                var sagaDatas = session.Advanced.DocumentQuery<RavenJObject>()
                    .Search("Originator", "*", EscapeQueryOptions.AllowAllWildcards)
                    .ToList();
                patchCommands.AddRange(from sagaData in sagaDatas
                    let id = session.Advanced.GetDocumentId(sagaData)
                    select new PatchCommandData
                    {
                        Key = id, Patches = new[]
                        {
                            new PatchRequest
                            {
                                Name = "Originator", Value = sagaData["Originator"].Value<string>().ReplaceMachineName(destinationServerName)
                            }
                        }
                    });
            }
            _store.DatabaseCommands.Batch(patchCommands);
        }
    }
}