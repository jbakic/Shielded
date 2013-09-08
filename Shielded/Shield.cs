using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

namespace Shielded
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
                AssertInTransaction();
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

        public static void AssertInTransaction()
        {
            if (_currentTransactionStartStamp == null)
                throw new InvalidOperationException("Operation needs to be in a transaction.");
        }

        private class Commute
        {
            public Action Perform;
            public ICommutableShielded[] Affecting;
        }

        private class TransItems
        {
            public HashSet<IShielded> Enlisted = new HashSet<IShielded>();
            public List<SideEffect> Fx;
            public List<Commute> Commutes;

            public void UnionWith(TransItems other)
            {
                Enlisted.UnionWith(other.Enlisted);
                if (other.Fx != null && other.Fx.Count > 0)
                    if (Fx == null)
                        Fx = new List<SideEffect>(other.Fx);
                    else
                        Fx.AddRange(other.Fx);
//                if (other.Commutes != null && other.Commutes.Count > 0)
//                    if (Commutes == null)
//                        Commutes = new List<Commute>(other.Commutes);
//                    else
//                        Commutes.AddRange(other.Commutes);
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
                if (items.Commutes != null)
                    items.Commutes.Clear();
                _itemPool.Add(items);
            }
        }

        private static VersionList _transactions = new VersionList();
        [ThreadStatic]
        private static TransItems _localItems;

        internal static void Enlist(IShielded item)
        {
            AssertInTransaction();
            if (!_localItems.Enlisted.Add(item))
                return;
            // does a commute have to degenerate?
            if (_localItems.Commutes != null && _localItems.Commutes.Count > 0)
            {
                var commutes = _localItems.Commutes.Where(c => c.Affecting.Contains(item)).ToArray();
                if (commutes.Length > 0)
                {
                    // first remove all, because they will most likely cause enlists and trigger
                    // each other if not removed.
                    _localItems.Commutes.RemoveAll(c => c.Affecting.Contains(item));
                    foreach (var comm in commutes)
                        comm.Perform();
                }
            }
        }

        [ThreadStatic]
        private static bool _blockCommute;

        /// <summary>
        /// The action is performed just before commit, and reads the latest
        /// data. If it conflicts, only it is retried. If it succeeds,
        /// we (try to) commit with the same write stamp along with it.
        /// The affecting param determines the IShieldeds that this transaction must
        /// not access, otherwise this commute must degenerate - it gets executed
        /// now, or at the moment when one of these IShieldeds enlists.
        /// </summary>
        internal static void EnlistCommute(Action perform, params ICommutableShielded[] affecting)
        {
            if (affecting == null || affecting.Length == 0)
                throw new ArgumentException();
            AssertInTransaction();

            if (_blockCommute || _localItems.Enlisted.Overlaps(affecting))
                perform(); // immediate degeneration. should be some warning.
            else
            {
                if (_localItems.Commutes == null)
                    _localItems.Commutes = new List<Commute>();
                _localItems.Commutes.Add(new Commute()
                {
                    Perform = perform,
                    Affecting = affecting
                });
            }
        }

        /// <summary>
        /// Conditional transaction. Does not execute immediately! Test is executed once just to
        /// get a read pattern, result is ignored. It will later be re-executed when any of the
        /// accessed IShieldeds commits.
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
                var items = IsolatedRun(() => test());
                _subscriptions.Append(new Shielded<CommitSubscription>(new CommitSubscription()
                {
                    Items = items.Enlisted,
                    Test = test,
                    Trans = trans
                }));
            });
        }

        public static void SideEffect(Action fx, Action rollbackFx = null)
        {
            if (_localItems.Fx == null)
                _localItems.Fx = new List<SideEffect>();
            _localItems.Fx.Add(new SideEffect(fx, rollbackFx));
        }

        public static T InTransaction<T>(Func<T> act)
        {
            T retVal = default(T);
            Shield.InTransaction(() => { retVal = act(); });
            return retVal;
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
                _localItems = TransItems.BagOrNew();
                _currentTransactionStartStamp = _transactions.SafeAdd(
                    () => Interlocked.Read(ref _lastStamp));

                try
                {
                    act();
                    if (!DoCommit())
                        repeat = true;
                }
                catch (TransException ex)
                {
                    if (_currentTransactionStartStamp.HasValue)
                    {
                        repeat = !(ex is NoRepeatTransException);
                        DoRollback();
                    }
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
            _blockCommute = true;
            try
            {
                act();
            }
            finally
            {
                oldItems.UnionWith(isolated);
                _localItems = oldItems;
                _blockCommute = false;
            }
            return isolated;
        }

        #region Conditional impl

        private struct CommitSubscription
        {
            public HashSet<IShielded> Items;
            public Func<bool> Test;
            public Func<bool> Trans;
        }
        private static ShieldedSeq<Shielded<CommitSubscription>> _subscriptions = new ShieldedSeq<Shielded<CommitSubscription>>();

        private static void TriggerSubscriptions(IShielded[] changes)
        {
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

        static bool CommitCheck(out long? writeStamp, out ICollection<IShielded> toCommit)
        {
            var items = _localItems;
            List<IShielded> enlisted = items.Enlisted.ToList();
            TransItems commutedItems = null;
            List<IShielded> commEnlisted = null;
            long oldStamp = _currentTransactionStartStamp.Value;
            bool commit = true;
            try
            {
                bool brokeInCommutes = true;
                do
                {
                    commit = true;
                    // first perform the commutes, if any, in a "sub-transaction". each commute may
                    // involve waiting for a spinlock to be released, this is why out of lock is better.
                    if (items.Commutes != null && items.Commutes.Any())
                    {
                        while (true)
                        {
                            _currentTransactionStartStamp = Interlocked.Read(ref _lastStamp);
                            commutedItems = TransItems.BagOrNew();
                            try
                            {
                                _localItems = commutedItems;
                                _blockCommute = true;
                                foreach (var comm in items.Commutes)
                                    comm.Perform();
                            }
                            catch (TransException ex)
                            {
                                if (ex is NoRepeatTransException)
                                    throw;
                                foreach (var item in commutedItems.Enlisted)
                                    item.Rollback();
                                TransItems.Bag(commutedItems);
                                commutedItems = null;
                                continue;
                            }
                            finally
                            {
                                _localItems = items;
                                _blockCommute = false;
                            }
                            break;
                        }
                        if (commutedItems.Enlisted.Overlaps(enlisted))
                            throw new ApplicationException("Incorrect commute affecting list, conflict with transaction.");
                        commEnlisted = commutedItems.Enlisted.ToList();
                    }

                    int rolledBack = -1;
                    lock (_stampLock)
                    {
                        writeStamp = Interlocked.Read(ref _lastStamp) + 1;

                        if (commEnlisted != null)
                        {
                            for (int i = 0; i < commEnlisted.Count; i++)
                                if (!commEnlisted[i].CanCommit(writeStamp.Value))
                                {
                                    commit = false;
                                    rolledBack = i - 1;
                                    for (int j = rolledBack; j >= 0; j--)
                                        commEnlisted[j].Rollback(writeStamp.Value);
                                    break;
                                }
                        }

                        if (commit)
                        {
                            _currentTransactionStartStamp = oldStamp;
                            brokeInCommutes = false;
                            for (int i = 0; i < enlisted.Count; i++)
                                if (!enlisted[i].CanCommit(writeStamp.Value))
                                {
                                    commit = false;
                                    rolledBack = i - 1;
                                    for (int j = i - 1; j >= 0; j--)
                                        enlisted[j].Rollback(writeStamp.Value);
                                    if (commEnlisted != null)
                                        for (int j = 0; j < commEnlisted.Count; j++)
                                            commEnlisted[j].Rollback(writeStamp.Value);
                                    break;
                                }

                            if (commit)
                                Interlocked.Increment(ref _lastStamp);
                        }
                    }
                    if (!commit)
                    {
                        if (brokeInCommutes)
                            for (int i = rolledBack + 1; i < commEnlisted.Count; i++)
                                commEnlisted[i].Rollback(null);
                        else
                            for (int i = rolledBack + 1; i < enlisted.Count; i++)
                                enlisted[i].Rollback(null);
                    }
                } while (brokeInCommutes);

                if (commEnlisted != null)
                    enlisted.InsertRange(0, commEnlisted);
                toCommit = enlisted;
                return commit;
            }
            finally
            {
                if (_currentTransactionStartStamp != oldStamp)
                    _currentTransactionStartStamp = oldStamp;
                if (commutedItems != null)
                {
                    _localItems.UnionWith(commutedItems);
                    TransItems.Bag(commutedItems);
                }
            }
        }

        private static bool DoCommit()
        {
            var items = _localItems;
            bool hasChanges = items.Enlisted.Any(s => s.HasChanges) ||
                (items.Commutes != null && items.Commutes.Count > 0);

            long? writeStamp = null;
            ICollection<IShielded> enlisted = null;
            bool commit = true;
            if (!hasChanges)
            {
                foreach (var item in items.Enlisted)
                    item.Commit(null);

                _transactions.Remove(_currentTransactionStartStamp.Value);
                _currentTransactionStartStamp = null;
                _localItems = null;

                if (items.Fx != null)
                    foreach (var fx in items.Fx)
                        // caller beware.
                        fx.Commit();
            }
            else if (CommitCheck(out writeStamp, out enlisted))
            {
                var trigger = enlisted.Where(s => s.HasChanges).ToArray();
                var copies = new List<IShielded>();
                foreach (var item in enlisted)
                    if (item.Commit(writeStamp))
                        copies.Add(item);
                RegisterCopies(writeStamp.Value, copies);

                _transactions.Remove(_currentTransactionStartStamp.Value);
                _currentTransactionStartStamp = null;
                _localItems = null;

                TriggerSubscriptions(trigger);

                if (items.Fx != null)
                    foreach (var fx in items.Fx)
                        // caller beware.
                        fx.Commit();
            }
            else
            {
                commit = false;

                _transactions.Remove(_currentTransactionStartStamp.Value);
                _currentTransactionStartStamp = null;
                _localItems = null;

                if (items.Fx != null)
                    foreach (var fx in items.Fx)
                        fx.Rollback();
            }

            TransItems.Bag(items);
            TrimCopies();
            return commit;
        }

        private static void DoRollback()
        {
            _transactions.Remove(_currentTransactionStartStamp.Value);
            var items = _localItems;
            _localItems = null;
            foreach (var item in items.Enlisted)
                item.Rollback();
            _currentTransactionStartStamp = null;

            if (items.Fx != null)
                foreach (var fx in items.Fx)
                    fx.Rollback();

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
            if (Interlocked.CompareExchange(ref _trimFlag, 1, 0) != 0)
                return;
            try
            {
                var lastStamp = Interlocked.Read(ref _lastStamp);
                var minTransactionNo = _transactions.Min() ?? lastStamp;

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

