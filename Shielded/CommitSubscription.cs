using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Shielded
{
    /// <summary>
    /// Contains information about a commit subscription, used in implementing
    /// <see cref="Shield.Conditional"/> and <see cref="Shield.PreCommit"/>.
    /// </summary>
    internal class CommitSubscription : IDisposable, IShielded
    {
        private readonly Shielded<SimpleHashSet> _items = new Shielded<SimpleHashSet>();
        private readonly Func<bool> Test;
        private readonly Action Trans;
        private readonly CommitSubscriptionContext Context;

        public CommitSubscription(CommitSubscriptionContext context, Func<bool> test, Action trans)
        {
            Context = context;
            Test = test;
            Trans = trans;
            Shield.InTransaction(() => {
                var items = Shield.IsolatedRun(() => Test());
                if (!items.Any())
                    throw new InvalidOperationException(
                        "Test function must access at least one shielded field.");
                _items.Value = items;
                UpdateEntries();
            });
        }

        public void Dispose()
        {
            Shield.InTransaction(() => {
                _items.Value = null;
                UpdateEntries();
            });
        }

        // runs, updates and commits itself, all in one.
        public void Run(IEnumerable<IShielded> trigger)
        {
            Shield.InTransaction(() =>
            {
                var oldItems = _items.Value;
                if (oldItems == null || !oldItems.Overlaps(trigger))
                    return; // not subscribed anymore..

                bool test = false;
                var testItems = Shield.IsolatedRun(() => test = Test());

                if (test) Trans();

                // _locals will have value if Trans() called Dispose on "itself".
                if (!_locals.HasValue && !testItems.SetEquals(oldItems))
                {
                    if (!testItems.Any())
                    {
                        // get rid of all entries first.
                        Remover(oldItems);
                        throw new InvalidOperationException(
                            "A conditional test function must access at least one shielded field.");
                    }
                    _items.Value = testItems;
                    UpdateEntries();
                }
            });
        }

        private class Locals
        {
            public List<IShielded> PreAdd;
            public List<IShielded> CommitRemove;
        }
        private readonly TransactionalStorage<Locals> _locals = new TransactionalStorage<Locals>();

        /// <summary>
        /// Updates our entries in the dictionary. To be immediately visible as soon as
        /// the _items.CanCommit() passes, we add ourselves into the dict straight away,
        /// and in a side effect we remove unnecessary entries, or, on rollback, undo
        /// the early additions.
        /// </summary>
        void UpdateEntries()
        {
            Shield.Enlist(this, false, true);
            var l = new Locals();

            var oldItems = _items.GetOldValue();
            var newItems = _items.Value;
            l.PreAdd = newItems == null ? null :
                (oldItems != null ? newItems.Except(oldItems).ToList() : newItems.ToList());
            l.CommitRemove = oldItems == null ? null :
                (newItems != null ? oldItems.Except(newItems).ToList() : oldItems.ToList());
            try { }
            finally
            {
                _locals.Value = l;
                var newArray = new[] { this };
                // early adding
                if (l.PreAdd != null)
                    foreach (var newKey in l.PreAdd)
                    {
                        lock (Context)
                            Context.AddOrUpdate(newKey,
                                k => newArray,
                                (k, existing) => existing.Concat(newArray).ToArray());
                    }
            }
        }

        void Remover(IEnumerable<IShielded> toRemove)
        {
            foreach (var remKey in toRemove)
            {
                lock (Context)
                {
                    var newList = WithoutMeOnce(Context[remKey]).ToArray();
                    if (newList.Length > 0)
                        Context[remKey] = newList;
                    else
                    {
                        IEnumerable<CommitSubscription> oldList;
                        Context.TryRemove(remKey, out oldList);
                    }
                }
            }
        }

        IEnumerable<CommitSubscription> WithoutMeOnce(IEnumerable<CommitSubscription> source)
        {
            bool notSkipped = true;
            foreach (var item in source)
            {
                if (notSkipped && item == this)
                {
                    notSkipped = false;
                    continue;
                }
                yield return item;
            }
        }

        #region IShielded implementation
        bool IShielded.CanCommit(WriteStamp writeStamp)
        {
            return true;
        }

        void IShielded.Commit()
        {
            if (_locals.Value.CommitRemove != null)
                Remover(_locals.Value.CommitRemove);
            _locals.Release();
        }

        void IShielded.Rollback()
        {
            if (!_locals.HasValue)
                return;
            if (_locals.Value.PreAdd != null)
                Remover(_locals.Value.PreAdd);
            _locals.Release();
        }

        void IShielded.TrimCopies(long smallestOpenTransactionId) { }

        bool IShielded.HasChanges
        {
            get
            {
                return true;
            }
        }

        object IShielded.Owner
        {
            get
            {
                return this;
            }
        }
        #endregion
    }
}

