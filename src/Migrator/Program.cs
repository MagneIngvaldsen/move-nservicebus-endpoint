using System;
using NServiceBus.TimeoutPersisters.RavenDB;

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
            timeOutMigrator.Migrate(ravenUrl, apiKey, dataBaseName, destinationServerName);
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
}