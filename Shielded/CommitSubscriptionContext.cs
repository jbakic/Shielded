using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Shielded
{
    internal class CommitSubscriptionContext : ConcurrentDictionary<IShielded, IEnumerable<CommitSubscription>>
    {
        public static readonly CommitSubscriptionContext PreCommit = new CommitSubscriptionContext();
        public static readonly CommitSubscriptionContext PostCommit = new CommitSubscriptionContext();

        /// <summary>
        /// Prepares subscriptions for execution based on the items that were committed.
        /// </summary>
        public IEnumerable<Action> Trigger(IEnumerable<IShielded> changes)
        {
            HashSet<CommitSubscription> result = null;
            foreach (var item in changes)
            {
                IEnumerable<CommitSubscription> list;
                if (!TryGetValue(item, out list) || list == null) continue;
                if (result == null) result = new HashSet<CommitSubscription>();
                result.UnionWith(list);
            }

            return result != null ?
                result.Select(cs => (Action)( () => cs.Run(changes) )) : null;
        }
    }
}

