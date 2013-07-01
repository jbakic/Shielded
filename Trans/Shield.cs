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
        private static List<Tuple<long, List<IShielded>>> _copiesByVersion = new List<Tuple<long, List<IShielded>>>();

        private static void RegisterCopies(long version, List<IShielded> copies)
        {
            int i;
            lock (_copiesByVersion)
            {
                for (i=0; i < _copiesByVersion.Count && _copiesByVersion[i].Item1 < version; i++)
                    ;
                _copiesByVersion.Insert(i, new Tuple<long, List<IShielded>>(version, copies));
            }
        }

        private static void TrimCopies()
        {
            lock (_copiesByVersion)
            {
                var keys = _transactionItems.Keys;
                var minTransactionNo = keys.Any() ? keys.Min() : Interlocked.Read(ref _lastStamp);

                int toDelete = 0;
                for (; toDelete < _copiesByVersion.Count &&
                     _copiesByVersion[toDelete].Item1 < minTransactionNo; toDelete++)
                    ;
                if (toDelete > 1)
                {
                    toDelete--;
                    foreach (var sh in _copiesByVersion.Take(toDelete).SelectMany(copy => copy.Item2).Distinct())
                        sh.TrimCopies(minTransactionNo);
                    _copiesByVersion.RemoveRange(0, toDelete);
                }
//#if DEBUG
//                Console.WriteLine("Copies trimmed for min stamp {0}. Count of lists of copies: {1}.",
//                    minTransactionNo, _copiesByVersion.Count);
//#endif
            }
        }

        private static ConcurrentDictionary<long, HashSet<IShielded>> _transactionItems
            = new ConcurrentDictionary<long, HashSet<IShielded>>();

        internal static void Enlist(IShielded item)
        {
            // reading the current stamp throws ...
            _transactionItems[CurrentTransactionStartStamp].Add(item);
        }

        public static void SideEffect(Action fx, Action rollbackFx = null)
        {
            Enlist(new SideEffect(fx, rollbackFx));
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
            HashSet<IShielded> items = _transactionItems[CurrentTransactionStartStamp];
            var itemCopy = new HashSet<IShielded>(items);
            _subscriptions.Append(new Shielded<CommitSubscription>(new CommitSubscription()
            {
                Items = itemCopy,
                Test = test,
                Trans = trans
            }));
        }

        private static void TriggerSubscriptions(IShielded[] changes)
        {
            if (!changes.Any())
                return;
            Shield.InTransaction(() =>
            {
                int i = 0;
                foreach (var sub in _subscriptions.ToArray())
                {
                    var subscription = sub.Read;
                    if (subscription.Items.Intersect(changes).Any())
                    {
                        bool test = false;
                        Func<bool> testFunc = subscription.Test;

                        HashSet<IShielded> oldItems = _transactionItems[CurrentTransactionStartStamp];
                        HashSet<IShielded> testItems = new HashSet<IShielded>();
                        _transactionItems[CurrentTransactionStartStamp] = testItems;
                        try
                        {
                            test = testFunc();
                        }
                        finally
                        {
                            foreach (var ti in testItems)
                                oldItems.Add(ti);
                            _transactionItems[CurrentTransactionStartStamp] = oldItems;
                        }

                        if (testItems.Count != subscription.Items.Count ||
                            testItems.Except(subscription.Items).Any())
                        {
                            sub.Modify((ref CommitSubscription cs) =>
                            {
                                cs.Items = testItems;
                            });
                        }
                        if (test && !sub.Read.Trans())
                            _subscriptions.RemoveAt(i);
                        else
                            i++;
                    }
                }
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
                    _currentTransactionStartStamp = Interlocked.Increment(ref _lastStamp);
                    _transactionItems[_currentTransactionStartStamp.Value] = new HashSet<IShielded>();
                }

                try
                {
                    act();
                    if (!DoCommit())
                        repeat = true;
                }
                catch (TransException ex)
                {
                    DoRollback();
                    repeat = !(ex is NoRepeatTransException);
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
            HashSet<IShielded> items = _transactionItems[CurrentTransactionStartStamp];
            List<IShielded> copies = new List<IShielded>();

            var changedItems = items.Where(s => s.HasChanges).ToArray();
            bool isStrict = changedItems.Any();
            long writeStamp;
            bool commit = true;
            var nonFx = items.Where(i => !(i is SideEffect)).ToArray();
            lock (_commitLock)
            {
                writeStamp = Interlocked.Increment(ref _lastStamp);
                foreach (var item in nonFx)
                    if (!item.CanCommit(isStrict, writeStamp))
                    {
                        commit = false;
                        foreach (var inner in nonFx)
                            inner.Rollback(writeStamp);
                        break;
                    }
            }

            _transactionItems.TryRemove(CurrentTransactionStartStamp, out items);
            if (commit)
            {
                // do the commit. out of global lock! and, non side-effects first.
                foreach (var item in nonFx)
                    if (item.Commit(writeStamp))
                        copies.Add(item);
                RegisterCopies(CurrentTransactionStartStamp, copies);

                _currentTransactionStartStamp = null;

                // if committing, trigger change subscriptions.
                TriggerSubscriptions(changedItems);

                foreach (var fx in items.Except(nonFx))
                    // caller beware.
                    fx.Commit(writeStamp);
            }
            else
            {
                _currentTransactionStartStamp = null;

                foreach (var item in items.OfType<SideEffect>())
                    item.Rollback(writeStamp);
            }

//#if DEBUG
//                Console.WriteLine("Stamp {0} commited with stamp {1}. Open count: {2}, item sets: {3}.",
//                    _currentTransactionStartStamp.Value, writeStamp, _openTransactions.Count,
//                    _transactionItems.Count);
//#endif

            TrimCopies();
            return commit;
        }

        private static void DoRollback()
        {
            HashSet<IShielded> items;
            _transactionItems.TryRemove(_currentTransactionStartStamp.Value, out items);
            foreach (var item in items.Where(i => !(i is SideEffect)))
                item.Rollback();
            _currentTransactionStartStamp = null;

            foreach (var item in items.OfType<SideEffect>())
                item.Rollback(null);

            TrimCopies();
        }
    }
}

