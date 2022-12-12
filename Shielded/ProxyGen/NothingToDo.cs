using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using System.Reflection;

namespace Shielded.ProxyGen
{
    class NothingToDo
    {
        public static bool With(Type t)
        {
            return !t.IsClass || t.IsSealed || !t.IsPublic ||
                t.GetConstructor(Type.EmptyTypes) == null ||
                !t.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                    .Any(IsInteresting);
        }

        internal static bool IsInteresting(PropertyInfo pi)
        {
            if (!pi.CanRead || !pi.CanWrite)
                return false;
            var access = pi.GetAccessors(true);
            // must have both accessors, both virtual, and both either public or protected, no mixing.
            if (access == null || access.Length != 2 || !access[0].IsVirtual)
                return false;
            if (access[0].IsPublic && access[1].IsFamily || access[0].IsFamily && access[1].IsPublic)
                throw new InvalidOperationException("Virtual property accessors must both be public, or both protected.");
            return access[0].IsPublic && access[1].IsPublic || access[0].IsFamily && access[1].IsFamily;
        }
    }
}
