using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Shielded.ProxyGen
{
    public static class Factory
    {
        private static Type ShieldedType(Type t)
        {
            if (NothingToDo.With(t))
                throw new InvalidOperationException(
                    "Unable to create proxy type - base type must be public and have virtual properties.");
            return ProxyGen.GetFor(t);
        }

        public static T NewShielded<T>() where T : class
        {
            return Activator.CreateInstance(ShieldedType(typeof(T))) as T;
        }
    }
}
