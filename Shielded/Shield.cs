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
        /// <summary>
        /// Current transaction's start stamp. Thread-static. Throws if called out of
        /// transaction.
        /// </summary>
        public static long CurrentTransactionStartStamp
        {
            get
            {
                return _currentTransactionStartStamp.Value;
            }
        }

        /// <summary>
        /// Gets a value indicating whether the current thread is in a transaction.
        /// </summary>
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

        private enum CommuteState
        {
            Ok = 0,
            Broken,
            Executed
        }

        private class Commute
        {
            public Action Perform;
            public ICommutableShielded[] Affecting;
            public CommuteState State;
        }

        private class TransItems
        {
            public HashSet<IShielded> Enlisted = new HashSet<IShielded>();
            public List<SideEffect> Fx;
            public List<Commute> Commutes;

            /// <summary>
            /// Unions the other items into this. Does not include commutes!
            /// </summary>
            public void UnionWith(TransItems other)
            {
                Enlisted.UnionWith(other.Enlisted);
                if (other.Fx != null && other.Fx.Count > 0)
                    if (Fx == null)
                        Fx = new List<SideEffect>(other.Fx);
                    else
                        Fx.AddRange(other.Fx);
            }
        }

        private static VersionList _transactions = new VersionList();
        [ThreadStatic]
        private static TransItems _localItems;
        [ThreadStatic]
        private static ICommutableShielded _blockEnlist;
        [ThreadStatic]
        private static bool _enforceTracking;
        [ThreadStatic]
        private static int? _commuteTime;

        /// <summary>
        /// Enlist the specified item in the transaction. Returns true if this is the
        /// first time in this transaction that this item is enlisted. hasLocals indicates
        /// if this item already has local storage prepared. If true, it means it must have
        /// enlisted already. However, in IsolatedRun you may have locals, even though you
        /// have not yet enlisted in the isolated items! So, this param is ignored within
        /// IsolatedRun calls. Outside of them, it allows for a much faster response.
        /// </summary>
        internal static bool Enlist(IShielded item, bool hasLocals)
        {
            AssertInTransaction();
            if (_blockEnlist != null && _blockEnlist != item)
                throw new InvalidOperationException("Accessing shielded fields in this context is forbidden.");
            if (!_enforceTracking && hasLocals)
                return false;
            if (_localItems.Enlisted.Add(item))
            {
                CheckCommutes(item);
                return true;
            }
            return false;
        }

        private static void CheckCommutes(IShielded item)
        {
            // does a commute have to degenerate?
            if (_localItems.Commutes == null || _localItems.Commutes.Count == 0)
                return;

            // in case one commute triggers others, we mark where we are in _comuteTime,
            // and no recursive call will execute commutes beyond that point.
            // so, clean "dependency resolution" - we trigger only those before us. those 
            // after us just get marked, and then we execute them (or, someone lower in the stack).
            var oldTime = _commuteTime;
            int execLimit = oldTime ?? _localItems.Commutes.Count;
            try
            {
                if (!oldTime.HasValue)
                    _blockCommute = true;
                for (int i = 0; i < _localItems.Commutes.Count; i++)
                {
                    var comm = _localItems.Commutes[i];
                    if (comm.State == CommuteState.Ok && comm.Affecting.Contains(item))
                        comm.State = CommuteState.Broken;
                    if (comm.State == CommuteState.Broken && i < execLimit)
                    {
                        _commuteTime = i;
                        comm.State = CommuteState.Executed;
                        comm.Perform();
                    }
                }
            }
            catch
            {
                // not sure if this matters, but i like it. please note that this and the Remove in finally
                // do not necessarily affect the same commutes.
                _localItems.Commutes.RemoveAll(c => c.Affecting.Contains(item));
                throw;
            }
            finally
            {
                _commuteTime = oldTime;
                if (!oldTime.HasValue)
                {
                    _blockCommute = false;
                    _localItems.Commutes.RemoveAll(c => c.State != CommuteState.Ok);
                }
            }
        }

        [ThreadStatic]
        private static bool _blockCommute;

        /// <summary>
        /// The strict version of EnlistCommute(), which will monitor that the code in
        /// perform does not enlist anything except the one item, affecting.
        /// </summary>
        internal static void EnlistStrictCommute(Action perform, ICommutableShielded affecting)
        {
            EnlistCommute(() => {
                try
                {
                    _blockEnlist = affecting;
                    perform();
                }
                finally
                {
                    _blockEnlist = null;
                }
            }, affecting);
        }

        /// <summary>
        /// The action is performed just before commit, and reads the latest
        /// data. If it conflicts, only it is retried. If it succeeds,
        /// we (try to) commit with the same write stamp along with it.
        /// The affecting param determines the IShieldeds that this transaction must
        /// not access, otherwise this commute must degenerate - it gets executed
        /// now, or at the moment when any of these IShieldeds enlists.
        /// </summary>
        internal static void EnlistCommute(Action perform, params ICommutableShielded[] affecting)
        {
            if (affecting == null)
                throw new ArgumentException();
            if (_blockEnlist != null &&
                (affecting.Length != 1 || affecting[0] != _blockEnlist))
                throw new InvalidOperationException("No shielded field access is allowed in this context.");
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
        /// Conditional transaction, which executes after some fields are committed into.
        /// Does not execute immediately! Test is executed once just to get a read pattern,
        /// result is ignored. It will later be re-executed when any of the accessed
        /// IShieldeds commits.
        /// When test passes, executes trans. Test is executed in a normal transaction. If it
        /// changes access patterns between calls, the subscription changes as well!
        /// Test and trans are executed in single transaction, and if the commit fails, test
        /// is also retried!
        /// </summary>
        /// <returns>An IDisposable which can be used to cancel the conditional by calling
        /// Dispose on it. Dispose can be called from trans.</returns>
        public static IDisposable Conditional(Func<bool> test, Action trans)
        {
            return new Subscription(SubscriptionContext.PostCommit, test, trans);
        }

#if (!NO_PRE_COMMIT)
        /// <summary>
        /// Pre-commit check, which executes just before commit of a transaction involving
        /// certain fields. Can be used to ensure certain invariants hold, for example.
        /// Does not execute immediately! Test is executed once just to get a read pattern,
        /// result is ignored. It will later be re-executed just before commit of any transaction
        /// that changes one of the fields it accessed.
        /// If test passes, executes trans as well. They will execute within the transaction
        /// that triggers them. If they are triggered by a transaction's commute, they will
        /// be called from within the commute sub-cycle (read stamp will be higher than in
        /// main transaction). They may even get called twice in the same transaction due
        /// to this, if the transaction changes one field normally, and commutes another.
        /// </summary>
        /// <returns>An IDisposable which can be used to cancel the conditional by calling
        /// Dispose on it. Dispose can be called from trans.</returns>
        public static IDisposable PreCommit(Func<bool> test, Action trans)
        {
            return new Subscription(SubscriptionContext.PreCommit, test, trans);
        }
#endif

        /// <summary>
        /// Enlists a side-effect - an operation to be performed only if the transaction
        /// commits. Optionally receives an action to perform in case of a rollback.
        /// If the transaction is rolled back, all enlisted side-effects are (also) cleared.
        /// </summary>
        public static void SideEffect(Action fx, Action rollbackFx = null)
        {
            if (_localItems.Fx == null)
                _localItems.Fx = new List<SideEffect>();
            _localItems.Fx.Add(new SideEffect(fx, rollbackFx));
        }

        /// <summary>
        /// Executes the function in a transaction, and returns it's final result. If the
        /// transaction fails (by calling Rollback(false)), returns the default value of type T.
        /// Nesting allowed.
        /// </summary>
        public static T InTransaction<T>(Func<T> act)
        {
            T retVal = default(T);
            Shield.InTransaction(() => { retVal = act(); });
            return retVal;
        }

        /// <summary>
        /// Executes the action in a transaction. Nesting allowed, it's a NOP.
        /// </summary>
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
                    _localItems = new TransItems();
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

        /// <summary>
        /// Rolls the transaction back. If given false as argument, there will be no repetitions.
        /// </summary>
        public static void Rollback(bool retry)
        {
            AssertInTransaction();
            throw (retry ?
                   new TransException("Requested rollback and retry.") :
                   new NoRepeatTransException("Requested rollback without retry."));
        }

        /// <summary>
        /// Runs the action, and returns a set of IShieldeds that the action enlisted.
        /// It will make sure to restore original enlisted items, merged with the ones
        /// that the action enlisted, before returning.
        /// </summary>
        internal static HashSet<IShielded> IsolatedRun(Action act)
        {
            var isolated = new TransItems();
            WithTransactionContext(isolated, act);
            return isolated.Enlisted;
        }

        #region Commit & rollback

        /// <summary>
        /// Create a local transaction context by replacing <see cref="Shield._localItems"/> with <paramref name="isolatedItems"/>
        /// and setting <see cref="Shield._blockCommute"/> to <c>false</c>, then perform <paramref name="act"/> and return
        /// with restored state. Forces all IShieldeds to enlist, regardless of previous enlist status.
        /// </summary>
        /// <param name="isolatedItems">The <see cref="TransItems"/> instance which temporarily replaces <see cref="Shield._localItems"/>.</param>
        /// <param name="merge">Whether to merge the isolated items into the original items when done. Defaults to <c>true</c>.
        /// If items are left unmerged, take care to handle TransExceptions and roll back the items yourself!</param>
        private static void WithTransactionContext(
            TransItems isolatedItems, Action act, bool merge = true)
        {
            if (_enforceTracking)
                throw new InvalidOperationException("WithTransactionContext does not nest!");
            var originalItems = _localItems;
            try
            {
                Shield._localItems = isolatedItems;
                Shield._blockCommute = true;
                Shield._enforceTracking = true;

                act();
            }
            finally
            {
                if (merge) originalItems.UnionWith(isolatedItems);
                Shield._localItems = originalItems;
                Shield._blockCommute = false;
                Shield._enforceTracking = false;
            }
        }

        /// <summary>
        /// Increases the current start stamp, and leaves the commuted items unmerged with the
        /// main transaction items!
        /// </summary>
        static void RunCommutes(out TransItems commutedItems)
        {
            var commutes = _localItems.Commutes;
            while (true)
            {
                _currentTransactionStartStamp = Interlocked.Read(ref _lastStamp);
                commutedItems = new TransItems();
                try
                {
                    WithTransactionContext(commutedItems, () =>
                    {
                        foreach (var comm in commutes)
                            comm.Perform();
                    }, merge: false);
                    return;
                }
                catch (TransException ex)
                {
                    foreach (var item in commutedItems.Enlisted)
                        item.Rollback();
                    commutedItems = null;

                    if (ex is NoRepeatTransException)
                        throw;
                }
            }
        }

        private static object _stampLock = new object();

        private static Func<IShielded, bool> HasChanges = i => i.HasChanges;

        static bool CommitCheck(out Tuple<int, long> writeStamp)
        {
            var items = _localItems;
            TransItems commutedItems = null;
            long oldStamp = _currentTransactionStartStamp.Value;
            bool commit;

#if (!NO_PRE_COMMIT)
            if (SubscriptionContext.PreCommit.Count > 0)
                SubscriptionContext.PreCommit
                    .Trigger(items.Enlisted.Where(HasChanges))
                    .Run();
#endif
            try
            {
                bool brokeInCommutes = items.Commutes != null && items.Commutes.Any();
                do
                {
                    commit = true;
                    if (brokeInCommutes)
                    {
                        RunCommutes(out commutedItems);
                        if (commutedItems.Enlisted.Overlaps(items.Enlisted))
                            throw new InvalidOperationException("Invalid commute - conflict with transaction.");
#if (!NO_PRE_COMMIT)
                        if (SubscriptionContext.PreCommit.Count > 0)
                            SubscriptionContext.PreCommit
                                .Trigger(commutedItems.Enlisted.Where(HasChanges))
                                .Run();
#endif
                    }

                    lock (_stampLock)
                    {
                        writeStamp = Tuple.Create(Thread.CurrentThread.ManagedThreadId,
                                                  Interlocked.Read(ref _lastStamp) + 1);
                        try
                        {
                            if (brokeInCommutes)
                            {
                                foreach (var item in commutedItems.Enlisted)
                                    if (!item.CanCommit(writeStamp))
                                    {
                                        commit = false;
                                        break;
                                    }
                                if (!commit) continue;
                            }

                            _currentTransactionStartStamp = oldStamp;
                            brokeInCommutes = false;
                            foreach (var item in items.Enlisted)
                                if (!item.CanCommit(writeStamp))
                                {
                                    commit = false;
                                    break;
                                }
                            if (!commit) break;

                            Interlocked.Increment(ref _lastStamp);
                        }
                        catch
                        {
                            commit = false;
                            throw;
                        }
                        finally
                        {
                            if (!commit)
                            {
                                if (commutedItems != null)
                                    foreach (var item in commutedItems.Enlisted)
                                        item.Rollback();
                                if (!brokeInCommutes)
                                    foreach (var item in items.Enlisted)
                                        item.Rollback();
                            }
                        }
                    }
                } while (brokeInCommutes);
                return commit;
            }
            finally
            {
                if (_currentTransactionStartStamp != oldStamp)
                    _currentTransactionStartStamp = oldStamp;
                // note that this changes the _localItems.Enlisted hashset to contain the
                // commute-enlists as well, regardless of the check outcome.
                if (commutedItems != null)
                    _localItems.UnionWith(commutedItems);
            }
        }

        private static bool DoCommit()
        {
            var items = _localItems;
            bool hasChanges = (items.Commutes != null && items.Commutes.Count > 0) ||
                items.Enlisted.Any(HasChanges);

            Tuple<int, long> writeStamp = null;
            bool commit = true;
            if (!hasChanges)
            {
                foreach (var item in items.Enlisted)
                    item.Commit();

                CloseTransaction();

                if (items.Fx != null)
                    items.Fx.Select(f => (Action)f.Commit).SafeRun();
            }
            else if (CommitCheck(out writeStamp))
            {
                var trigger = new List<IShielded>();
                // this must not be interrupted by a Thread.Abort!
                try { }
                finally
                {
                    foreach (var item in items.Enlisted)
                    {
                        if (item.HasChanges) trigger.Add(item);
                        item.Commit();
                    }
                    RegisterCopies(writeStamp.Item2, trigger);
                    CloseTransaction();
                }

                (items.Fx != null ? items.Fx.Select(f => (Action)f.Commit) : null)
                    .SafeConcat(SubscriptionContext.PostCommit.Trigger(trigger))
                    .SafeRun();
            }
            else
            {
                commit = false;
                CloseTransaction();

                if (items.Fx != null)
                    items.Fx.Select(f => (Action)f.Rollback).SafeRun();
            }

            TrimCopies();
            return commit;
        }

        private static IEnumerable<T> SafeConcat<T>(this IEnumerable<T> first, IEnumerable<T> second)
        {
            if (first != null && second != null)
                return first.Concat(second);
            else if (first != null)
                return first;
            else
                return second;
        }

        private static void DoRollback()
        {
            var items = _localItems;
            foreach (var item in items.Enlisted)
                item.Rollback();
            CloseTransaction();

            if (items.Fx != null)
                foreach (var fx in items.Fx)
                    items.Fx.Select(f => (Action)f.Rollback).SafeRun();

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
        private static int _trimClock = 0;

        private static void TrimCopies()
        {
            // trimming won't start every time..
            if ((Interlocked.Increment(ref _trimClock) & 0xF) != 0)
                return;

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
                while (_copiesByVersion.TryPeek(out curr) && curr.Item1 <= minTransactionNo)
                {
                    toTrim.UnionWith(curr.Item2);
                    _copiesByVersion.TryDequeue(out curr);
                }

                foreach (var sh in toTrim)
                    sh.TrimCopies(minTransactionNo);
            }
            finally
            {
                if (tookFlag)
                    Interlocked.Exchange(ref _trimFlag, 0);
            }
        }

        #endregion
    }
}

