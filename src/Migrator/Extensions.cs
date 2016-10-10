using System;
using System.Collections.Generic;
using System.Linq;

namespace Migrator
{
    internal static class Extensions
    {
        public static string ReplaceMachineName(this string input, string destinationServerName)
        {
            return input.Substring(0, input.IndexOf("@", StringComparison.Ordinal) + 1) + destinationServerName;
        }

        public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> items,
                                                       int maxItems)
        {
            return items.Select((item, inx) => new { item, inx })
                        .GroupBy(x => x.inx / maxItems)
                        .Select(g => g.Select(x => x.item));
        }
    }


}