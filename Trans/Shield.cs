using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

namespace Trans
{
    public static class Shield
    {
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

        // an object implementing IShielded..
        private class Token : IShielded
        {
            public bool CanCommit(bool strict, long writeStamp)
            {
                return true;
            }

            public bool Commit(long? writeStamp)
            {
                return false;
            }

            public void Rollback(long? writeStamp)
            {
            }

            public void TrimCopies(long smallestOpenTransactionId)
            {
            }

            public bool HasChanges
            {
                get
                {
                    return false;
                }
            }
        }
        private static readonly Token GiveUpToken = new Token();

        public static void GiveUp()
        {
            Enlist(GiveUpToken);
        }

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
            if (Interlocked.CompareExchange(ref _trimFlag, 1, 0) != 0)
                return;
            try
            {
                var keys = _transactionItems.Keys;
                var minTransactionNo = keys.Any() ? keys.Min() : Interlocked.Read(ref _lastStamp);

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
                _trimFlag = 0;
            }
//#if DEBUG
//                Console.WriteLine("Copies trimmed for min stamp {0}. Count of lists of copies: {1}.",
//                    minTransactionNo, _copiesByVersion.Count);
//#endif
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

        private static ConcurrentDictionary<long, TransItems> _transactionItems
            = new ConcurrentDictionary<long, TransItems>();

        internal static void Enlist(IShielded item)
        {
            // reading the current stamp throws ...
            _transactionItems[CurrentTransactionStartStamp].Enlisted.Add(item);
        }

        public static void SideEffect(Action fx, Action rollbackFx = null)
        {
            //Enlist(new SideEffect(fx, rollbackFx));
            var items = _transactionItems[CurrentTransactionStartStamp];
            if (items.Fx == null)
                items.Fx = new List<Trans.SideEffect>();
            items.Fx.Add(new SideEffect(fx, rollbackFx));
        }

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
                Items = new HashSet<IShielded>(_transactionItems[CurrentTransactionStartStamp].Enlisted),
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
            TransItems oldItems = _transactionItems[CurrentTransactionStartStamp];
            var isolated = TransItems.BagOrNew();
            _transactionItems[CurrentTransactionStartStamp] = isolated;
            try
            {
                act();
            }
            finally
            {
                oldItems.UnionWith(isolated);
                _transactionItems[CurrentTransactionStartStamp] = oldItems;
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
                // for stable reading, numbers cannot be obtained while transaction commit is going on.
                lock (_commitLock)
                {
                    _currentTransactionStartStamp = Interlocked.Read(ref _lastStamp) + 1;
                    _transactionItems[_currentTransactionStartStamp.Value] = TransItems.BagOrNew();
                    Interlocked.Increment(ref _lastStamp);
                }

                try
                {
                    act();
                    if (_transactionItems[CurrentTransactionStartStamp].Enlisted.Contains(GiveUpToken))
                        DoRollback();
                    else if (!DoCommit())
                        repeat = true;
                }
                catch (TransException ex)
                {
                    repeat = !(ex is NoRepeatTransException) &&
                        !_transactionItems[CurrentTransactionStartStamp].Enlisted.Contains(GiveUpToken);
                    DoRollback();
                }
                finally
                {
                    if (_currentTransactionStartStamp.HasValue)
                        DoRollback();
                }
            } while (repeat);
        }

        private static object _commitLock = new object();

        private static bool DoCommit()
        {
            // if there are items, they must all commit.
            var items = _transactionItems[_currentTransactionStartStamp.Value];

            var enlisted = items.Enlisted.ToArray();
            bool hasChanges = enlisted.Any(s => s.HasChanges);
            long? writeStamp = null;
            bool commit = true;

            if (hasChanges)
                lock (_commitLock)
                {
                    writeStamp = Interlocked.Increment(ref _lastStamp);
                    foreach (var item in enlisted)
                        if (!item.CanCommit(hasChanges, writeStamp.Value))
                        {
                            commit = false;
                            foreach (var inner in enlisted)
                                inner.Rollback(writeStamp);
                            break;
                        }
                }

            if (commit)
            {
                // do the commit. out of global lock! and, non side-effects first.
                List<IShielded> copies = hasChanges ? new List<IShielded>() : null;
                IShielded[] trigger = hasChanges ? enlisted.Where(s => s.HasChanges).ToArray() : null;
                foreach (var item in enlisted)
                    if (item.Commit(writeStamp) && copies != null)
                        copies.Add(item);
                if (copies != null)
                    RegisterCopies(writeStamp.Value, copies);

                _transactionItems.TryRemove(_currentTransactionStartStamp.Value, out items);
                _currentTransactionStartStamp = null;

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
                _transactionItems.TryRemove(_currentTransactionStartStamp.Value, out items);
                _currentTransactionStartStamp = null;

                if (items.Fx != null)
                    foreach (var item in items.Fx)
                        item.Rollback(writeStamp);
            }

//#if DEBUG
//                Console.WriteLine("Stamp {0} commited with stamp {1}. Open count: {2}, item sets: {3}.",
//                    _currentTransactionStartStamp.Value, writeStamp, _openTransactions.Count,
//                    _transactionItems.Count);
//#endif

            TransItems.Bag(items);
            TrimCopies();
            return commit;
        }

        private static void DoRollback()
        {
            TransItems items;
            _transactionItems.TryRemove(_currentTransactionStartStamp.Value, out items);
            foreach (var item in items.Enlisted)
                item.Rollback();
            _currentTransactionStartStamp = null;

            if (items.Fx != null)
                foreach (var item in items.Fx)
                    item.Rollback(null);

            TransItems.Bag(items);
            TrimCopies();
        }
    }
}

