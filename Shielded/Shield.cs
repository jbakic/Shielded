using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

namespace Shielded
{
    /// <summary>
    /// Just a reference for users to be able to cancel conditionals. It's contents are
    /// visible to the Shield only.
    /// </summary>
    public class ConditionalHandle
    {
        internal ConditionalHandle() {}
    }

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

            /// <summary>
            /// Bag the items, for this or other threads to reuse. Will clear your
            /// reference before adding the items into the bag, for safety.
            /// </summary>
            public static void Bag(ref TransItems items)
            {
                items.Enlisted.Clear();
                if (items.Fx != null)
                    items.Fx.Clear();
                if (items.Commutes != null)
                    items.Commutes.Clear();
                var t = items;
                items = null;
                _itemPool.Add(t);
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
            if (!_blockCommute &&
                _localItems.Commutes != null && _localItems.Commutes.Count > 0)
            {
                // commutes will, if untouched, execute one by one in the sub-transaction before commit.
                // so, the safest thing, since any of them can read whatever, is to execute them all!
                // otherwise, behaviour is inconsistent.
                // it's better not to construct a Func with a closure here, that takes time! manual checking:
                int i = 0;
                for (; i < _localItems.Commutes.Count; i++)
                    if (_localItems.Commutes[i].Affecting.Contains(item))
                        break;
                if (i == _localItems.Commutes.Count) return;
                try
                {
                    _blockCommute = true;
                    foreach (var comm in _localItems.Commutes)
                        comm.Perform();
                    _localItems.Commutes.Clear();
                }
                finally
                {
                    _blockCommute = false;
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
        public static ConditionalHandle Conditional(Func<bool> test, Func<bool> trans)
        {
            return Shield.InTransaction(() =>
            {
                var items = IsolatedRun(() => test());
                var sub = new Shielded<CommitSubscription>(new CommitSubscription()
                {
                    Items = items.Enlisted,
                    Test = test,
                    Trans = trans
                });
                AddSubscription(sub);
                return new ConditionalHandleInternal() { Sub = sub };
            });
        }

        /// <summary>
        /// Removes the subscription of a previously made Conditional.
        /// </summary>
        public static void CancelConditional(ConditionalHandle handle)
        {
            Shield.InTransaction(() => {
                RemoveSubscription(((ConditionalHandleInternal)handle).Sub);
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
                try
                {
                    _localItems = TransItems.BagOrNew();
                    // this should not be interrupted by an Abort. the moment between
                    // adding the version into the list, and writing it into _current..
                    try { }
                    finally
                    {
                        _currentTransactionStartStamp = _transactions.SafeAdd(
                            () => Interlocked.Read(ref _lastStamp));
                    }

                    act();
                    if (!DoCommit())
                        repeat = true;
                }
                catch (TransException ex)
                {
                    if (_currentTransactionStartStamp.HasValue)
                        repeat = !(ex is NoRepeatTransException);
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
            var isolated = TransItems.BagOrNew();
            WithTransactionContext(isolated, act);
            return isolated;
        }

        #region Conditional impl

        private class ConditionalHandleInternal : ConditionalHandle
        {
            public Shielded<CommitSubscription> Sub;
        }

        private struct CommitSubscription
        {
            public HashSet<IShielded> Items;
            public Func<bool> Test;
            public Func<bool> Trans;
        }
        private static ShieldedDict<IShielded, ShieldedSeq<Shielded<CommitSubscription>>> _subscriptions =
            new ShieldedDict<IShielded, ShieldedSeq<Shielded<CommitSubscription>>>();

        private static void AddSubscription(Shielded<CommitSubscription> sub, IEnumerable<IShielded> items = null)
        {
            foreach (var item in items != null ? items : sub.Read.Items)
            {
                ShieldedSeq<Shielded<CommitSubscription>> l;
                if (!_subscriptions.TryGetValue(item, out l))
                    _subscriptions.Add(item, new ShieldedSeq<Shielded<CommitSubscription>>(sub));
                else
                    l.Append(sub);
            }
        }

        private static void RemoveSubscription(Shielded<CommitSubscription> sub, IEnumerable<IShielded> items = null)
        {
            foreach (var item in items != null ? items : sub.Read.Items)
            {
                var l = _subscriptions[item];
                if (l.Count == 1)
                    _subscriptions.Remove(item);
                else
                    l.Remove(sub);
            }
            if (items == null)
                sub.Modify((ref CommitSubscription cs) => { cs.Items = null; });
        }

        private static void TriggerSubscriptions(IShielded[] changes)
        {
            HashSet<Shielded<CommitSubscription>> triggered = null;
            Shield.InTransaction(() =>
            {
                // for speed, when conditionals are not used.
                if (_subscriptions.Count == 0)
                    return;

                foreach (var item in changes)
                {
                    ShieldedSeq<Shielded<CommitSubscription>> l;
                    if (_subscriptions.TryGetValue(item, out l))
                    {
                        if (triggered == null)
                            triggered = new HashSet<Shielded<CommitSubscription>>(l);
                        else
                            triggered.UnionWith(l);
                    }
                }
            });

            if (triggered != null)
                foreach (var sub in triggered)
                    Shield.InTransaction(() =>
                    {
                        var subscription = sub.Read;
                        if (subscription.Items == null) return; // not subscribed anymore..

                        bool test = false;
                        var testItems = IsolatedRun(() => test = subscription.Test());

                        if (test && !subscription.Trans())
                        {
                            RemoveSubscription(sub);
                            TransItems.Bag(ref testItems);
                        }
                        else if (!testItems.Enlisted.SetEquals(subscription.Items))
                        {
                            RemoveSubscription(sub, subscription.Items.Except(testItems.Enlisted));
                            AddSubscription(sub, testItems.Enlisted.Except(subscription.Items));
                            sub.Modify((ref CommitSubscription cs) =>
                            {
                                cs.Items = testItems.Enlisted;
                            });
                        }
                        else
                            TransItems.Bag(ref testItems);
                    });
        }

        #endregion

        #region Commit & rollback

        /// <summary>
        /// Create a local transaction context by replacing <see cref="Shield._localItems"/> with <paramref name="isolatedItems"/>
        /// and setting <see cref="Shield._blockCommute"/> to <c>false</c>, then perform <paramref name="act"/> and return
        /// with restored state.
        /// </summary>
        /// <param name="isolatedItems">The <see cref="TransItems"/> instance which temporarily replaces <see cref="Shield._localItems"/>.</param>
        /// <param name="merge">Whether to merge the isolated items into the original items when done. Defaults to <c>true</c>.
        /// If items are left unmerged, take care to handle TransExceptions and roll back the items yourself!</param>
        private static void WithTransactionContext(
            TransItems isolatedItems, Action act, bool merge = true)
        {
            var originalItems = _localItems;
            try
            {
                Shield._localItems = isolatedItems;
                Shield._blockCommute = true;

                act();
            }
            finally
            {
                if (merge) originalItems.UnionWith(isolatedItems);
                Shield._localItems = originalItems;
                Shield._blockCommute = false;
            }
        }

        /// <summary>
        /// Increases the current start stamp, and leaves the commuted items unmerged with the
        /// main transaction items!
        /// </summary>
        static void RunCommutes(out TransItems commutedItems)
        {
            var items = _localItems;
            while (true)
            {
                _currentTransactionStartStamp = Interlocked.Read(ref _lastStamp);
                commutedItems = TransItems.BagOrNew();
                try
                {
                    WithTransactionContext(commutedItems, () =>
                    {
                        foreach (var comm in items.Commutes)
                            comm.Perform();
                    }, merge: false);
                    return;
                }
                catch (TransException ex)
                {
                    foreach (var item in commutedItems.Enlisted)
                        item.Rollback();
                    TransItems.Bag(ref commutedItems);

                    if (ex is NoRepeatTransException)
                        throw;
                }
            }
        }

        private static object _stampLock = new object();

        static bool CommitCheck(out Tuple<int, long> writeStamp, out ICollection<IShielded> toCommit)
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
                        RunCommutes(out commutedItems);
                        // if in conflict with main trans, it has advantage, i.e. the criteria for intersecting
                        // fields is the stricter one.
                        commutedItems.Enlisted.ExceptWith(enlisted);
                        commEnlisted = commutedItems.Enlisted.ToList();
                    }

                    int rolledBack = -1;
                    lock (_stampLock)
                    {
                        writeStamp = Tuple.Create(Thread.CurrentThread.ManagedThreadId,
                                                  Interlocked.Read(ref _lastStamp) + 1);
                        try
                        {
                            if (commEnlisted != null)
                            {
                                for (int i = 0; i < commEnlisted.Count; i++)
                                    if (!commEnlisted[i].CanCommit(writeStamp))
                                    {
                                        commit = false;
                                        rolledBack = i - 1;
                                        for (int j = rolledBack; j >= 0; j--)
                                            commEnlisted[j].Rollback();
                                        break;
                                    }
                            }

                            if (commit)
                            {
                                _currentTransactionStartStamp = oldStamp;
                                brokeInCommutes = false;
                                for (int i = 0; i < enlisted.Count; i++)
                                    if (!enlisted[i].CanCommit(writeStamp))
                                    {
                                        commit = false;
                                        rolledBack = i - 1;
                                        for (int j = i - 1; j >= 0; j--)
                                            enlisted[j].Rollback();
                                        if (commEnlisted != null)
                                            for (int j = 0; j < commEnlisted.Count; j++)
                                                commEnlisted[j].Rollback();
                                        break;
                                    }

                                if (commit)
                                    Interlocked.Increment(ref _lastStamp);
                            }
                        }
                        catch
                        {
                            // we roll them all back. it's good to get rid of the write lock.
                            if (commEnlisted != null)
                                foreach (var item in commEnlisted)
                                    item.Rollback();
                            if (!brokeInCommutes)
                                foreach (var item in enlisted)
                                    item.Rollback();
                            throw;
                        }
                    }
                    if (!commit)
                    {
                        if (brokeInCommutes)
                            for (int i = rolledBack + 1; i < commEnlisted.Count; i++)
                                commEnlisted[i].Rollback();
                        else
                            for (int i = rolledBack + 1; i < enlisted.Count; i++)
                                enlisted[i].Rollback();
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
                    TransItems.Bag(ref commutedItems);
                }
            }
        }

        private static bool DoCommit()
        {
            var items = _localItems;
            bool hasChanges = items.Enlisted.Any(s => s.HasChanges) ||
                (items.Commutes != null && items.Commutes.Count > 0);

            Tuple<int, long> writeStamp = null;
            ICollection<IShielded> enlisted = null;
            bool commit = true;
            if (!hasChanges)
            {
                foreach (var item in items.Enlisted)
                    item.Commit();

                CloseTransaction();

                if (items.Fx != null)
                    foreach (var fx in items.Fx)
                        // caller beware.
                        fx.Commit();
            }
            else if (CommitCheck(out writeStamp, out enlisted))
            {
                var trigger = enlisted.Where(s => s.HasChanges).ToArray();
                // this must not be interrupted by a Thread.Abort!
                try { }
                finally
                {
                    RegisterCopies(writeStamp.Item2, trigger);
                    foreach (var item in enlisted)
                        item.Commit();
                    CloseTransaction();
                }

                TriggerSubscriptions(trigger);

                if (items.Fx != null)
                    foreach (var fx in items.Fx)
                        // caller beware.
                        fx.Commit();
            }
            else
            {
                commit = false;
                CloseTransaction();

                if (items.Fx != null)
                    foreach (var fx in items.Fx)
                        fx.Rollback();
            }

            TransItems.Bag(ref items);
            TrimCopies();
            return commit;
        }

        private static void DoRollback()
        {
            var items = _localItems;
            foreach (var item in items.Enlisted)
                item.Rollback();
            CloseTransaction();

            if (items.Fx != null)
                foreach (var fx in items.Fx)
                    fx.Rollback();

            TransItems.Bag(ref items);
            TrimCopies();
        }

        private static void CloseTransaction()
        {
            try { }
            finally
            {
                _transactions.Remove(_currentTransactionStartStamp.Value);
                _currentTransactionStartStamp = null;
                _localItems = null;
            }
        }



        // the long is their current version, but being in this list indicates they have something older.
        private static ConcurrentQueue<Tuple<long, IEnumerable<IShielded>>> _copiesByVersion =
            new ConcurrentQueue<Tuple<long, IEnumerable<IShielded>>>();

        private static void RegisterCopies(long version, IEnumerable<IShielded> copies)
        {
            if (copies.Any())
                _copiesByVersion.Enqueue(Tuple.Create(version, copies));
        }

        private static int _trimFlag = 0;
        private static void TrimCopies()
        {
            bool tookFlag = false;
            try
            {
                try { }
                finally
                {
                    tookFlag = Interlocked.CompareExchange(ref _trimFlag, 1, 0) == 0;
                }
                if (!tookFlag) return;

                var lastStamp = Interlocked.Read(ref _lastStamp);
                var minTransactionNo = _transactions.Min() ?? lastStamp;

                Tuple<long, IEnumerable<IShielded>> curr;
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
                if (tookFlag)
                    Interlocked.Decrement(ref _trimFlag);
            }
        }

        #endregion
    }
}

