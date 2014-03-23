using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Shielded
{
    /// <summary>
    /// Contains information about a commit subscription, used in implementing
    /// <see cref="Shield.Conditional"/>. Internally keeps a dictionary to find out
    /// which instances are triggered by a certain commit.
    /// </summary>
    internal class CommitSubscription : IDisposable
    {
        private readonly Shielded<HashSet<IShielded>> _items = new Shielded<HashSet<IShielded>>();
        private readonly Func<bool> Test;
        private readonly Func<bool> Trans;

        public CommitSubscription(Func<bool> test, Func<bool> trans)
        {
            Test = test;
            Trans = trans;
            Shield.InTransaction(() => {
                var items = Shield.IsolatedRun(() => Test());
                if (!items.Any())
                    throw new InvalidOperationException(
                        "A conditional test function must access at least one shielded field.");
                _items.Assign(items);
                UpdateEntries();
            });
        }

        public void Dispose()
        {
            Shield.InTransaction(() => {
                _items.Assign(null);
                UpdateEntries();
            });
        }


        private static ConcurrentDictionary<IShielded, IEnumerable<CommitSubscription>> _dict =
            new ConcurrentDictionary<IShielded, IEnumerable<CommitSubscription>>();

        /// <summary>
        /// Runs the subscriptions triggered by the given changes. Should be called outside of
        /// a transaction.
        /// </summary>
        public static void Trigger(IList<IShielded> changes)
        {
            if (_dict.Count == 0)
                return;

            HashSet<CommitSubscription> result = null;
            foreach (var item in changes)
            {
                IEnumerable<CommitSubscription> list;
                if (!_dict.TryGetValue(item, out list) || list == null) continue;
                if (result == null) result = new HashSet<CommitSubscription>();
                result.UnionWith(list);
            }

            if (result != null)
                foreach (var sub in result)
                    sub.Run(changes);
        }

        // runs, updates and commits itself, all in one. called out of transaction.
        private void Run(IList<IShielded> trigger)
        {
            Shield.InTransaction(() =>
            {
                var oldItems = _items.Read;
                if (oldItems == null || !oldItems.Overlaps(trigger))
                    return; // not subscribed anymore..

                bool test = false;
                var testItems = Shield.IsolatedRun(() => test = Test());

                if (test && !Trans())
                {
                    _items.Assign(null);
                    UpdateEntries();
                }
                else if (!testItems.SetEquals(oldItems))
                {
                    if (!testItems.Any())
                    {
                        // get rid of all entries first.
                        Remover(oldItems);
                        throw new InvalidOperationException(
                            "A conditional test function must access at least one shielded field.");
                    }
                    _items.Assign(testItems);
                    UpdateEntries();
                }
            });
        }

        /// <summary>
        /// Updates our entries in the dictionary. To be immediately visible as soon as
        /// the _items.CanCommit() passes, we add ourselves into the dict straight away,
        /// and in a side effect we remove unnecessary entries, or, on rollback, undo
        /// the early additions.
        /// </summary>
        void UpdateEntries()
        {
            var oldItems = _items.GetOldValue();
            var newItems = _items.Read;
            var preAdd = newItems == null ? null :
                (oldItems != null ? newItems.Except(oldItems).ToList() : newItems.ToList());
            var commitRemove = oldItems == null ? null :
                (newItems != null ? oldItems.Except(newItems).ToList() : oldItems.ToList());
            try { }
            finally
            {
                // early adding
                if (preAdd != null)
                    foreach (var newKey in preAdd)
                    {
                        _dict.AddOrUpdate(newKey, k => Enumerable.Repeat(this, 1),
                            (k, existing) => {
                                var list = existing != null ?
                                    new List<CommitSubscription>(existing) : new List<CommitSubscription>();
                                list.Add(this);
                                return list;
                            });
                    }
                // on commit, remove no longer needed entries, or on rollback, remove the preAdd entries
                Shield.SideEffect(
                    commitRemove != null ? () => Remover(commitRemove) : (Action)null,
                    preAdd != null ? () => Remover(preAdd) : (Action)null);
            }
        }

        void Remover(IEnumerable<IShielded> toRemove)
        {
            foreach (var remKey in toRemove)
            {
                IEnumerable<CommitSubscription> oldList;
                List<CommitSubscription> newList;
                do
                {
                    if (!_dict.TryGetValue(remKey, out oldList)) break;
                    if (oldList.Count() > 1)
                    {
                        newList = new List<CommitSubscription>(oldList);
                        newList.Remove(this);
                    }
                    else
                        newList = null;
                }
                while (!_dict.TryUpdate(remKey, newList, oldList));
            }
        }
    }
}

