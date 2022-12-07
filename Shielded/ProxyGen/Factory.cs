using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Shielded.ProxyGen
{
    public class Factory
    {
        static ConcurrentDictionary<Type, Type> proxies = new ConcurrentDictionary<Type, Type>();

        public static bool IsProxy(Type t)
        {
            return t.BaseType != null && proxies.ContainsKey(t.BaseType);
        }

        public static T NewShielded<T>()
        {
            var type = typeof(T);
            var shieldType = type.Assembly.GetType(type.Namespace + ".__shielded" + type.Name);
            //var shieldType = type.Assembly.GetType("ConsoleTests.__shieldedSimpleEntity");
            if (shieldType != null)
                return (T)Activator.CreateInstance(shieldType);
            return default;
        }

        public static void PrepareTypes(Type[] types)
        {
            if (!types.Any())
                return;
            if (types.Any(NothingToDo.With))
                throw new InvalidOperationException(
                    "Unable to make proxies for types: " +
                    string.Join(", ", types.Where(NothingToDo.With)));

            foreach (var type in types)
            {
                var shieldType = type.Assembly.GetType(type.Namespace + ".__shielded" + type.Name);
                if(shieldType != null)
                    proxies.TryAdd(type, shieldType);
                else
                    throw new InvalidOperationException($"Unable to make proxies for type: {type.FullName} without [Shield] Attribute");
            }
        }
    }
}
