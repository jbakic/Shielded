using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

namespace Trans
{
    public static class Shield
    {
        private static long _lastStamp;

        [ThreadStatic]
        private static long? _currentTransactionStartStamp;
        public static long CurrentTransactionStartStamp
        {
            get
            {
                if (_currentTransactionStartStamp == null)
                    throw new InvalidOperationException("Operation needs to be in a transaction.");
                return _currentTransactionStartStamp.Value;
            }
        }

        public static bool IsInTransaction
        {
            get
            {
                return _currentTransactionStartStamp.HasValue;
            }
        }

        private class TransItems
        {
            public HashSet<IShielded> Enlisted = new HashSet<IShielded>();
            public List<SideEffect> Fx;

            public void UnionWith(TransItems other)
            {
                Enlisted.UnionWith(other.Enlisted);
                if (other.Fx != null)
                    if (Fx == null)
                        Fx = new List<SideEffect>(other.Fx);
                    else
                        Fx.AddRange(other.Fx);
            }

            private TransItems() {}

            private static ConcurrentBag<TransItems> _itemPool = new ConcurrentBag<TransItems>();
            public static TransItems BagOrNew()
            {
                TransItems result;
                if (_itemPool.TryTake(out result))
                    return result;
                return new TransItems();
            }

            public static void Bag(TransItems items)
            {
                items.Enlisted.Clear();
                if (items.Fx != null)
                    items.Fx.Clear();
                _itemPool.Add(items);
            }
        }

        private static ConcurrentDictionary<long, int> _transactions
            = new ConcurrentDictionary<long, int>();
        [ThreadStatic]
        private static TransItems _localItems;

        internal static void Enlist(IShielded item)
        {
            // reading the current stamp throws ...
            _localItems.Enlisted.Add(item);
        }

        /// <summary>
        /// Conditional transaction. If test returns true, executes immediately.
        /// If not, test is re-executed when any of the accessed IShieldeds commits.
        /// When test passes, executes trans. While trans returns true, subscription is maintained.
        /// Test is executed in a normal transaction. If it changes access patterns between
        /// calls, the subscription changes as well!
        /// Test and trans are executed in single transaction, and if the commit fails, test
        /// is also retried!
        /// </summary>
        public static void Conditional(Func<bool> test, Func<bool> trans)
        {
            Shield.InTransaction(() =>
            {
                if (test())
                { // important brackets! :)
                    if (trans())
                        Shield.SideEffect(() => Conditional(test, trans));
                }
                else
                    Subscribe(test, trans);
            });
        }

        public static void SideEffect(Action fx, Action rollbackFx = null)
        {
            if (_localItems.Fx == null)
                _localItems.Fx = new List<SideEffect>();
            _localItems.Fx.Add(new SideEffect(fx, rollbackFx));
        }

        public static void InTransaction(Action act)
        {
            if (_currentTransactionStartStamp.HasValue)
            {
                act();
                return;
            }

            bool repeat;
            do
            {
                repeat = false;
                // trimming uses lastStamp if the items are empty, so we suspend it momentarily if it's not running
                // already. O(1)!
                Interlocked.Increment(ref _trimFlag);
                try
                {
                    _currentTransactionStartStamp = Interlocked.Read(ref _lastStamp);
                    _localItems = TransItems.BagOrNew();
                    _transactions.AddOrUpdate(_currentTransactionStartStamp.Value, 1, (_, i) => i + 1);
                }
                finally
                {
                    Interlocked.Decrement(ref _trimFlag);
                }

                try
                {
                    act();
                    if (!DoCommit())
                        repeat = true;
                }
                catch (TransException ex)
                {
                    repeat = !(ex is NoRepeatTransException);
                    DoRollback();
                }
                finally
                {
                    if (_currentTransactionStartStamp.HasValue)
                        DoRollback();
                }
            } while (repeat);
        }

        private class NoRepeatTransException : TransException
        {
            public NoRepeatTransException(string message) : base(message) {}
        }

        public static void Rollback(bool retry)
        {
            throw (retry ?
                   new TransException("Requested rollback and retry.") :
                   new NoRepeatTransException("Requested rollback without retry."));
        }

        #region Conditional impl

        private struct CommitSubscription
        {
            public HashSet<IShielded> Items;
            public Func<bool> Test;
            public Func<bool> Trans;
        }
        private static ShieldedSeq<Shielded<CommitSubscription>> _subscriptions = new ShieldedSeq<Shielded<CommitSubscription>>();

        private static void Subscribe(Func<bool> test, Func<bool> trans)
        {
            _subscriptions.Append(new Shielded<CommitSubscription>(new CommitSubscription()
            {
                Items = new HashSet<IShielded>(_localItems.Enlisted),
                Test = test,
                Trans = trans
            }));
        }

        /// <summary>
        /// Runs the action, and returns a set of IShieldeds that the action enlisted.
        /// It will make sure to restore original enlisted items, merged with the ones
        /// that the action enlisted, before returning.
        /// </summary>
        private static TransItems IsolatedRun(Action act)
        {
            TransItems oldItems = _localItems;
            var isolated = TransItems.BagOrNew();
            _localItems = isolated;
            try
            {
                act();
            }
            finally
            {
                oldItems.UnionWith(isolated);
                _localItems = oldItems;
            }
            return isolated;
        }

        private static void TriggerSubscriptions(IShielded[] changes)
        {
            if (!changes.Any())
                return;

            Shielded<CommitSubscription>[] triggered = null;
            Shield.InTransaction(() =>
            {
                triggered = _subscriptions.Where(s => s.Read.Items.Overlaps(changes)).ToArray();
            });

            foreach (var sub in triggered)
                Shield.InTransaction(() =>
                {
                    int i = _subscriptions.IndexOf(sub);
                    if (i < 0) return; // not subscribed anymore..

                    var subscription = sub.Read;
                    bool test = false;
                    var testItems = IsolatedRun(() => test = subscription.Test());

                    if (!testItems.Enlisted.SetEquals(subscription.Items))
                    {
                        sub.Modify((ref CommitSubscription cs) =>
                        {
                            cs.Items = testItems.Enlisted;
                        });
                    }
                    else
                        TransItems.Bag(testItems);
                    if (test && !sub.Read.Trans())
                        _subscriptions.RemoveAt(i);
                });
        }

        #endregion

        #region Commit & rollback

        private static object _stampLock = new object();

        private static void Unregister(long stamp)
        {
            int n;
            do
            {
                n = _transactions[stamp];
            }
            while (!_transactions.TryUpdate(stamp, n-1, n));
        }

        private static bool DoCommit()
        {
            // if there are items, they must all commit.
            var items = _localItems;

            var enlisted = items.Enlisted.ToArray();
            bool hasChanges = enlisted.Any(s => s.HasChanges);
            long? writeStamp = null;
            bool commit = true;

            if (hasChanges)
            {
                int rolledBack = 0;
                lock (_stampLock)
                {
                    writeStamp = Interlocked.Read(ref _lastStamp) + 1;
                    for (int i = 0; i < enlisted.Length; i++)
                        if (!enlisted[i].CanCommit(writeStamp.Value))
                        {
                            commit = false;
                            rolledBack = i - 1;
                            for (int j = i - 1; j >= 0; j--)
                                enlisted[j].Rollback(writeStamp.Value);
                            break;
                        }
                    Interlocked.Increment(ref _lastStamp);
                }
                if (!commit)
                    for (int i = rolledBack + 1; i < enlisted.Length; i++)
                        enlisted[i].Rollback(null);
            }

            if (commit)
            {
                // do the commit. out of global lock! and, non side-effects first.
                List<IShielded> copies = hasChanges ? new List<IShielded>() : null;
                IShielded[] trigger = hasChanges ? enlisted.Where(s => s.HasChanges).ToArray() : null;
                for (int i = 0; i < enlisted.Length; i++)
                    if (enlisted[i].Commit(writeStamp) && copies != null)
                        copies.Add(enlisted[i]);
                if (copies != null)
                    RegisterCopies(writeStamp.Value, copies);

                Unregister(_currentTransactionStartStamp.Value);
                _currentTransactionStartStamp = null;
                _localItems = null;

                // if committing, trigger change subscriptions.
                if (trigger != null)
                    TriggerSubscriptions(trigger);

                if (items.Fx != null)
                    foreach (var fx in items.Fx)
                        // caller beware.
                        fx.Commit(writeStamp);
            }
            else
            {
                Unregister(_currentTransactionStartStamp.Value);
                _currentTransactionStartStamp = null;
                _localItems = null;

                if (items.Fx != null)
                    foreach (var fx in items.Fx)
                        fx.Rollback(writeStamp);
            }

            TransItems.Bag(items);
            TrimCopies();
            return commit;
        }

        private static void DoRollback()
        {
            Unregister(_currentTransactionStartStamp.Value);
            var items = _localItems;
            _localItems = null;
            foreach (var item in items.Enlisted)
                item.Rollback();
            _currentTransactionStartStamp = null;

            if (items.Fx != null)
                foreach (var fx in items.Fx)
                    fx.Rollback(null);

            TransItems.Bag(items);
            TrimCopies();
        }



        // the long is their current version, but being in this list indicates they have something older.
        private static ConcurrentQueue<Tuple<long, List<IShielded>>> _copiesByVersion =
            new ConcurrentQueue<Tuple<long, List<IShielded>>>();

        private static void RegisterCopies(long version, List<IShielded> copies)
        {
            if (copies.Any())
                _copiesByVersion.Enqueue(new Tuple<long, List<IShielded>>(version, copies));
        }

        private static int _trimFlag = 0;
        private static void TrimCopies()
        {
            // must read value before checking flag, because InTransaction raises flag
            // while opening. it does this since there is a small period of time when
            // _lastStamp could become bigger then his start stamp (i.e. two transactions
            // starting simultaneously), and items don't contain his items yet.
            var lastStamp = Interlocked.Read(ref _lastStamp);
            if (Interlocked.CompareExchange(ref _trimFlag, 1, 0) != 0)
                return;
            try
            {
                var keys = _transactions.Keys;
                var minTransactionNo = keys.Any() ? keys.Min() : lastStamp;

                Tuple<long, List<IShielded>> curr;
                HashSet<IShielded> toTrim = new HashSet<IShielded>();
                while (_copiesByVersion.TryPeek(out curr))
                {
                    if (curr.Item1 < minTransactionNo)
                    {
                        toTrim.UnionWith(curr.Item2);
                        _copiesByVersion.TryDequeue(out curr);
                    }
                    else
                        break;
                }

                foreach (var sh in toTrim)
                    sh.TrimCopies(minTransactionNo);
            }
            finally
            {
                Interlocked.Decrement(ref _trimFlag);
            }
        }

        #endregion
    }
}

