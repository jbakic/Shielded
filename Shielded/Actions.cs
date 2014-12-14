using System;
using System.Collections.Generic;
using System.Linq;

namespace Shielded
{
    internal static class Actions
    {
        /// <summary>
        /// Executes an IEnumerable of actions, each in its own try/catch block. If any
        /// throw, it will still run every one, and then afterwards throw an
        /// AggregateException.
        /// </summary>
        public static void SafeRun(this IEnumerable<Action> actions)
        {
            if (actions == null) return;

            List<Exception> exceptions = null;
            foreach (var act in actions)
            {
                try
                {
                    if (act != null) act();
                }
                catch (Exception ex)
                {
                    if (exceptions == null) exceptions = new List<Exception>();
                    exceptions.Add(ex);
                }
            }
            if (exceptions != null)
                throw new AggregateException(exceptions);
        }

        /// <summary>
        /// Executes an IEnumerable of actions, with no special handling. First exception
        /// interrupts it.
        /// </summary>
        public static void Run(this IEnumerable<Action> actions)
        {
            if (actions == null) return;
            foreach (var act in actions)
                act();
        }
    }
}

