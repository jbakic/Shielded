using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Shielded.ProxyGen
{
    /// <summary>
    /// Helper for the ProxyGen for resolving all references of a certain assembly.
    /// CodeDOM is a bit silly, it needs us to specify every single assembly, even the
    /// indirectly referenced ones. It does not seem to be able to find them by itself.
    /// And it also does not tolerate duplicates, which makes this rather complicated.
    /// </summary>
    internal static class ReferenceResolver
    {
        public static Dictionary<string, Assembly> GetReferencesRecursive(this Assembly source)
        {
            var leftToAdd = GetReferences(source);
            var result = new Dictionary<string, Assembly>();
            while (leftToAdd.Any())
            {
                var next = leftToAdd.First();
                leftToAdd.Remove(next.Key);
                result.Add(next.Key, next.Value);
                foreach (var refOfRef in GetReferences(next.Value))
                {
                    if (result.ContainsKey(refOfRef.Key))
                    {
                        if (IsLesserVersion(result[refOfRef.Key], refOfRef.Value))
                        {
                            // we want to re-process the newer version...
                            result.Remove(refOfRef.Key);
                            leftToAdd.Add(refOfRef.Key, refOfRef.Value);
                        }
                    }
                    else if (!leftToAdd.ContainsKey(refOfRef.Key) || IsLesserVersion(leftToAdd[refOfRef.Key], refOfRef.Value))
                    {
                        leftToAdd[refOfRef.Key] = refOfRef.Value;
                    }
                }
            }
            return result;
        }

        private static bool IsLesserVersion(Assembly a, Assembly b)
        {
            return a.GetName().Version < b.GetName().Version;
        }

        private static Dictionary<string, Assembly> GetReferences(Assembly source)
        {
            return source.GetReferencedAssemblies()
                .ToDictionary(name => name.Name, name => Assembly.ReflectionOnlyLoad(name.FullName));
        }
    }
}
