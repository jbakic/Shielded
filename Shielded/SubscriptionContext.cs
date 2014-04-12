using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Shielded
{
    internal class SubscriptionContext : ConcurrentDictionary<IShielded, IEnumerable<Subscription>>
    {
        public static readonly SubscriptionContext PreCommit = new SubscriptionContext();
        public static readonly SubscriptionContext PostCommit = new SubscriptionContext();

//        private readonly bool UpdateItemsOnTest;
//
//        public SubscriptionContext(bool updateItems)
//        {
//            UpdateItemsOnTest = updateItems;
//        }
//
        /// <summary>
        /// Prepares subscriptions for execution based on the items that were committed.
        /// </summary>
        public IEnumerable<Action> Trigger(IEnumerable<IShielded> changes)
        {
            if (Count == 0)
                return null;

            HashSet<Subscription> result = null;
            foreach (var item in changes)
            {
                IEnumerable<Subscription> list;
                if (!TryGetValue(item, out list) || list == null) continue;
                if (result == null) result = new HashSet<Subscription>();
                result.UnionWith(list);
            }

            return result != null ?
                result.Select(cs => (Action)( () => cs.Run(changes) ) ) : null;
        }
    }
}

