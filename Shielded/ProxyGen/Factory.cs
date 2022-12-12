using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;

namespace Shielded.ProxyGen
{
    public class Factory
    {
        static ConcurrentDictionary<Type, Type> proxies = new ConcurrentDictionary<Type, Type>();

        public static bool IsProxy(Type type)
        {
            if (type.BaseType != null)
            {
                if (proxies.ContainsKey(type.BaseType))
                    return true;
                return HasShieldedAttribute(type.BaseType);
            }
            return false;
        }

        private static string GetSubProxyName(Type type)
        {
            if (string.IsNullOrEmpty(type.Namespace))
            {
                return "__shielded" + type.Name;
            }
            else
                return type.Namespace + ".__shielded" + type.Name;
        }

        public static T NewShielded<T>()
        {
            var type = typeof(T);
            if (proxies.TryGetValue(type, out Type shieldType))
            {
                return (T)Activator.CreateInstance(shieldType);
            }
            else
            {
                shieldType = type.Assembly.GetType(GetSubProxyName(type));
                if (shieldType != null)
                {
                    proxies.TryAdd(type, shieldType);
                    return (T)Activator.CreateInstance(shieldType);
                }
                else
                {
                    throw new InvalidOperationException($"Unable to make proxies for type: {type.FullName} without [Shielded] Attribute");
                }
            }
        }

        /// <summary>
        /// warm up for Shielded pocos
        /// </summary>
        /// <param name="types"></param>
        /// <exception cref="InvalidOperationException"></exception>
        public static void PrepareTypes(Type[] types)
        {
            if (!types.Any())
                return;
            foreach (var type in types)
            {
                if (!proxies.TryGetValue(type, out Type shieldType))
                {
                    shieldType = type.Assembly.GetType(type.Namespace + ".__shielded" + type.Name);
                    if (shieldType != null)
                        proxies.TryAdd(type, shieldType);
                    else
                        throw new InvalidOperationException($"Unable to make proxies for type: {type.FullName} without [Shielded] Attribute");
                }
            }
        }

        /// <summary>
        /// warm up for Shielded pocos
        /// </summary>
        /// <param name="asm"></param>
        /// <exception cref="InvalidOperationException"></exception>
        public static void PrepareTypes(Assembly asm)
        {
            if (asm == null)
                return;
            foreach (var type in asm.GetTypes())
            {
                if (HasShieldedAttribute(type))
                {
                    var shieldType = type.Assembly.GetType(type.Namespace + ".__shielded" + type.Name);
                    if (shieldType != null)
                        proxies.TryAdd(type, shieldType);
                    else
                        throw new InvalidOperationException($"Unable to make proxies for type: {type.FullName} without [Shielded] Attribute");
                }
            }
        }

        public static bool HasShieldedAttribute(Type target)
        {
            if (target == null)
                return false;
            Type cur = target;
            do
            {
                var att = target.GetCustomAttributes(typeof(ShieldedAttribute), true);
                if (att != null && att.Length > 0)
                    return true;
                cur = cur.BaseType;
            }
            while (cur != null);
            return false;
        }

    }
}
