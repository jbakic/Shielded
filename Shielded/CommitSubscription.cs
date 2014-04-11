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
    internal class CommitSubscription : IDisposable, IShielded
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
        /// Prepares subscriptions for execution based on the items that were committed.
        /// </summary>
        public static IEnumerable<Action> Trigger(IList<IShielded> changes)
        {
            if (_dict.Count == 0)
                return Enumerable.Empty<Action>();

            HashSet<CommitSubscription> result = null;
            foreach (var item in changes)
            {
                IEnumerable<CommitSubscription> list;
                if (!_dict.TryGetValue(item, out list) || list == null) continue;
                if (result == null) result = new HashSet<CommitSubscription>();
                result.UnionWith(list);
            }

            return result != null ?
                result.Select(cs => (Action)( () => cs.Run(changes) ) ) :
                Enumerable.Empty<Action>();
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

        private class Locals
        {
            public List<IShielded> PreAdd;
            public List<IShielded> CommitRemove;
        }
        private readonly LocalStorage<Locals> _locals = new LocalStorage<Locals>();

        /// <summary>
        /// Updates our entries in the dictionary. To be immediately visible as soon as
        /// the _items.CanCommit() passes, we add ourselves into the dict straight away,
        /// and in a side effect we remove unnecessary entries, or, on rollback, undo
        /// the early additions.
        /// </summary>
        void UpdateEntries()
        {
            Shield.Enlist(this);
            var l = new Locals();

            var oldItems = _items.GetOldValue();
            var newItems = _items.Read;
            l.PreAdd = newItems == null ? null :
                (oldItems != null ? newItems.Except(oldItems).ToList() : newItems.ToList());
            l.CommitRemove = oldItems == null ? null :
                (newItems != null ? oldItems.Except(newItems).ToList() : oldItems.ToList());
            try { }
            finally
            {
                _locals.Value = l;
                // early adding
                if (l.PreAdd != null)
                    foreach (var newKey in l.PreAdd)
                    {
                        lock (_dict)
                            _dict.AddOrUpdate(newKey, k => Enumerable.Repeat(this, 1),
                                (k, existing) => {
                                    var list = existing != null ?
                                        new List<CommitSubscription>(existing) : new List<CommitSubscription>();
                                    list.Add(this);
                                    return list;
                                });
                    }
            }
        }

        void Remover(IEnumerable<IShielded> toRemove)
        {
            foreach (var remKey in toRemove)
            {
                IEnumerable<CommitSubscription> oldList;
                List<CommitSubscription> newList;
                lock (_dict)
                {
                    oldList = _dict[remKey];
                    if (oldList.Count() > 1)
                    {
                        newList = new List<CommitSubscription>(oldList);
                        newList.Remove(this);
                        _dict[remKey] = newList;
                    }
                    else
                        _dict.TryRemove(remKey, out oldList);
                }
            }
        }

        #region IShielded implementation
        bool IShielded.CanCommit(Tuple<int, long> writeStamp)
        {
            return true;
        }

        void IShielded.Commit()
        {
            if (_locals.Value.CommitRemove != null)
                Remover(_locals.Value.CommitRemove);
            _locals.Value = null;
        }

        void IShielded.Rollback()
        {
            if (!_locals.HasValue)
                return;
            if (_locals.Value.PreAdd != null)
                Remover(_locals.Value.PreAdd);
            _locals.Value = null;
        }

        void IShielded.TrimCopies(long smallestOpenTransactionId) { }

        bool IShielded.HasChanges
        {
            get
            {
                return true;
            }
        }
        #endregion
    }
}

