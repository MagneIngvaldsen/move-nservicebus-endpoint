using System;

namespace Migrator
{
    internal static class StringExtensions
    {
        public static string ReplaceMachineName(this string input, string destinationServerName)
        {
            return input.Substring(0, input.IndexOf("@", StringComparison.Ordinal) + 1) + destinationServerName;
        }
    }
}