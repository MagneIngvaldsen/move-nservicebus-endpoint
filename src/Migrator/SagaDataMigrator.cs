using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Json.Linq;

namespace Migrator
{
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
                var sagaDatas = new List<RavenJObject>();
                var start = 0;
                while (true)
                {
                    var current = session.Advanced.DocumentQuery<RavenJObject>()
                    .Search("Originator", "*", EscapeQueryOptions.AllowAllWildcards)
                            .Take(1024)
                            .Skip(start)
                            .ToList();
                    if (current.Count == 0)
                        break;

                    start += current.Count;
                    sagaDatas.AddRange(current);
                }

              
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