using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

namespace Shielded
{
    public static class Shield
    {
        [ThreadStatic]
        private static ReadTicket _readTicket;
        /// <summary>
        /// Current transaction's read stamp, i.e. the latest version it can read.
        /// Thread-static. Throws if called out of transaction.
        /// </summary>
        public static long ReadStamp
        {
            get
            {
                return _readTicket.Stamp;
            }
        }

        /// <summary>
        /// Gets a value indicating whether the current thread is in a transaction.
        /// </summary>
        public static bool IsInTransaction
        {
            get
            {
                return _readTicket != null;
            }
        }

        public static void AssertInTransaction()
        {
            if (_readTicket == null)
                throw new InvalidOperationException("Operation needs to be in a transaction.");
        }

        private enum CommuteState
        {
            Ok = 0,

            /// <summary>
            /// Commute must execute due to transaction accessing a field it works on.
            /// </summary>
            Broken,

            /// <summary>
            /// Commute was executed.
            /// </summary>
            Executed
        }

        private class Commute
        {
            /// <summary>
            /// The commutable action.
            /// </summary>
            public Action Perform;

            /// <summary>
            /// The fields which, if enlisted by the main transaction, must cause the commute to
            /// execute in-transaction.
            /// </summary>
            public ICommutableShielded[] Affecting;

            public CommuteState State;
        }

        private class TransItems
        {
            public ISet<IShielded> Enlisted =
#if USE_STD_HASHSET
                new HashSet<IShielded>();
#else
                new SimpleHashSet();
#endif
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

        /// <summary>
        /// Multi-set containing read stamps of all open transactions. Used to determine
        /// smallest open stamp when trimming old copies.
        /// </summary>
        //private static VersionList _transactions = new VersionList();

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
            if (_blockEnlist != null && _blockEnlist != item)
                throw new InvalidOperationException("Accessing shielded fields in this context is forbidden.");
            if (!_enforceTracking && hasLocals)
                return false;
            AssertInTransaction();
            if (!_localItems.Enlisted.Contains(item))
            {
                // must not add into Enlisted before we run commutes, otherwise commutes' calls
                // to Enlist would return false, and the commutes, although running first, would
                // not actually check the lock! this also means that CheckCommutes must tolerate
                // being reentered with the same item.
                CheckCommutes(item);
                return _localItems.Enlisted.Add(item);
            }
            return false;
        }

        /// <summary>
        /// When an item is enlisted for which we have defined commutes, it causes those
        /// commutes to be executed immediately. This happens before the field is read or
        /// written into. The algorithm here allows for one commute to trigger others,
        /// guaranteeing that they will all execute in correct order provided that each
        /// commute correctly defined the fields which, if accessed, must cause it to
        /// degenerate.
        /// </summary>
        private static void CheckCommutes(IShielded item)
        {
            if (_localItems.Commutes == null || _localItems.Commutes.Count == 0)
                return;

            // in case one commute triggers others, we mark where we are in _comuteTime,
            // and no recursive call will execute commutes beyond that point.
            // so, clean "dependency resolution" - we trigger only those before us. those 
            // after us just get marked, and then we execute them (or, someone lower in the stack).
            var oldTime = _commuteTime;
            var oldBlock = _blockCommute;
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
                    _blockCommute = oldBlock;
                    _localItems.Commutes.RemoveAll(c => c.State != CommuteState.Ok);
                }
            }
        }

        [ThreadStatic]
        private static bool _blockCommute;

        /// <summary>
        /// The strict version of EnlistCommute, which will monitor that the code in
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

        /// <summary>
        /// Pre-commit check, which executes just before commit of a transaction involving
        /// certain fields. Can be used to ensure certain invariants hold, for example.
        /// Does not execute immediately! Test is executed once just to get a read pattern,
        /// result is ignored. It will later be re-executed just before commit of any transaction
        /// that changes one of the fields it accessed.
        /// If test passes, executes trans as well. They will execute within the transaction
        /// that triggers them. If they access a commuted field, the commute will degenerate.
        /// </summary>
        /// <returns>An IDisposable which can be used to cancel the pre-commit by calling
        /// Dispose on it. Dispose can be called from trans.</returns>
        public static IDisposable PreCommit(Func<bool> test, Action trans)
        {
            return new Subscription(SubscriptionContext.PreCommit, test, trans);
        }

        /// <summary>
        /// Enlists a side-effect - an operation to be performed only if the transaction
        /// commits. Optionally receives an action to perform in case of a rollback.
        /// If the transaction is rolled back, all enlisted side-effects are (also) cleared.
        /// Should be used to perform all IO and such operations (except maybe logging).
        /// 
        /// If this is called out of transaction, the fx action (if one was provided)
        /// will be directly executed. This preserves correct behavior if the call finds
        /// itself sometimes in, sometimes out of transaction, because of some crazy
        /// nesting differences.
        /// </summary>
        public static void SideEffect(Action fx, Action rollbackFx = null)
        {
            if (!IsInTransaction)
            {
                if (fx != null) fx();
                return;
            }

            if (_localItems.Fx == null)
                _localItems.Fx = new List<SideEffect>();
            _localItems.Fx.Add(new SideEffect(fx, rollbackFx));
        }

        /// <summary>
        /// Executes the function in a transaction, and returns it's final result. Nesting allowed.
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
            if (IsInTransaction)
            {
                act();
                return;
            }

            do
            {
                try
                {
                    _localItems = new TransItems();
                    VersionList.GetReaderTicket(out _readTicket);

                    act();
                    if (DoCommit())
                        return;
                }
                catch (TransException) { }
                finally
                {
                    if (_readTicket != null)
                        DoRollback();
                }
            } while (true);
        }

        /// <summary>
        /// Rolls the transaction back and retries it from the beginning. If you don't
        /// want the transaction to repeat, throw and catch an exception yourself.
        /// </summary>
        public static void Rollback()
        {
            AssertInTransaction();
            throw new TransException("Requested rollback and retry.");
        }

        /// <summary>
        /// Runs the action, and returns a set of IShieldeds that the action enlisted.
        /// It will make sure to restore original enlisted items, merged with the ones
        /// that the action enlisted, before returning. This is important to make sure
        /// all thread-local state is cleared on transaction exit. The isolated action
        /// may still cause outer transaction's commutes to degenerate.
        /// </summary>
        internal static ISet<IShielded> IsolatedRun(Action act)
        {
            var isolated = new TransItems();
            isolated.Commutes = _localItems.Commutes;
            WithTransactionContext(isolated, act);
            return isolated.Enlisted;
        }

        #region Commit & rollback

        /// <summary>
        /// Create a local transaction context by replacing <see cref="Shield._localItems"/> with
        /// <paramref name="isolatedItems"/> and setting <see cref="Shield._blockCommute"/> to <c>true</c>,
        /// then perform <paramref name="act"/> and return with restored state. Forces all IShieldeds to
        /// enlist, regardless of previous enlist status.
        /// </summary>
        /// <param name="isolatedItems">The instance which temporarily replaces <see cref="Shield._localItems"/>.</param>
        /// <param name="merge">Whether to merge the isolated items into the original items when done. Defaults to <c>true</c>.
        /// If items are left unmerged, take care to handle TransExceptions and roll back the items yourself!</param>
        private static void WithTransactionContext(
            TransItems isolatedItems, Action act, bool merge = true)
        {
            var originalItems = _localItems;
            var oldEnforce = _enforceTracking;
            var oldBlock = _blockCommute;
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
                Shield._blockCommute = oldBlock;
                Shield._enforceTracking = oldEnforce;
            }
        }

        /// <summary>
        /// Executes the commutes, returning through the out param the set of items that the
        /// commutes had accessed.
        /// Increases the current start stamp, and leaves the commuted items unmerged with the
        /// main transaction items!
        /// </summary>
        static void RunCommutes(out TransItems commutedItems)
        {
            var commutes = _localItems.Commutes;
            while (true)
            {
                _readTicket = new ReadTicket() { Stamp = VersionList.GetUntrackedReadStamp() };
                commutedItems = new TransItems();
                try
                {
                    WithTransactionContext(commutedItems, () =>
                    {
                        for (int i = 0; i < commutes.Count; i++)
                            commutes[i].Perform();
                    }, merge: false);
                    return;
                }
                catch (TransException)
                {
#if USE_STD_HASHSET
                    foreach (var item in commutedItems.Enlisted)
                        item.Rollback();
#else
                    ((SimpleHashSet)commutedItems.Enlisted).Rollback();
#endif
                    commutedItems = null;
                }
            }
        }

        private static bool HasChanges(IShielded item)
        {
            return item.HasChanges;
        }

        static bool CommitCheck(out WriteTicket ticket)
        {
            ticket = null;
            var items = _localItems;
            TransItems commutedItems = null;
            var oldReadTicket = _readTicket;
            bool commit = false;
            bool brokeInCommutes = items.Commutes != null && items.Commutes.Count > 0;

            if (SubscriptionContext.PreCommit.Count > 0)
            {
                // if any commute would trigger a pre-commit check, this check could, if executed
                // in the commute sub-transaction, see newer values in fields which
                // were read (but not written to) by the main transaction. commutes are normally
                // very isolated to prevent this, but pre-commits we cannot isolate.
                // so, commutes trigger them now, and they cause the commutes to degenerate.
                SubscriptionContext.PreCommit
                    .Trigger(brokeInCommutes ?
                        items.Enlisted.Where(HasChanges).Concat(
                            items.Commutes.SelectMany(c => c.Affecting)) :
                        items.Enlisted.Where(HasChanges))
                    .Run();
            }

            var enlistedClone =
#if USE_STD_HASHSET
                new HashSet<IShielded>(items.Enlisted);
#else
                ((SimpleHashSet)items.Enlisted).SharingClone();
#endif

            try
            {
repeatCommutes: if (brokeInCommutes)
                {
                    RunCommutes(out commutedItems);
                    if (items.Enlisted.Overlaps(commutedItems.Enlisted))
                        throw new InvalidOperationException("Invalid commute - conflict with transaction.");
                }

//                lock (_stampLock)
                CommitLock theLock = null;
                var writeStamp = new WriteStamp(
                    Thread.CurrentThread.ManagedThreadId);
                try
                {
                    CommitLocker.Enter(
                        enlistedClone,
                        commutedItems != null ? commutedItems.Enlisted : null,
                        out theLock);

                    if (brokeInCommutes)
#if USE_STD_HASHSET
                        foreach (var item in commutedItems.Enlisted)
                            if (!item.CanCommit(writeStamp))
                                goto repeatCommutes;
#else
                        if (!((SimpleHashSet)commutedItems.Enlisted).CanCommit(writeStamp))
                            goto repeatCommutes;
#endif

                    _readTicket = oldReadTicket;
                    brokeInCommutes = false;
#if USE_STD_HASHSET
                    foreach (var item in items.Enlisted)
                        if (!item.CanCommit(writeStamp))
                            return false;
#else
                    if (!((SimpleHashSet)items.Enlisted).CanCommit(writeStamp))
                        return false;
#endif

                    commit = true;
                }
                finally
                {
                    if (!commit)
                    {
#if USE_STD_HASHSET
                        if (commutedItems != null)
                            foreach (var item in commutedItems.Enlisted)
                                item.Rollback();
                        if (!brokeInCommutes)
                            foreach (var item in items.Enlisted)
                                item.Rollback();
#else
                        if (commutedItems != null)
                            ((SimpleHashSet)commutedItems.Enlisted).Rollback();
                        if (!brokeInCommutes)
                            ((SimpleHashSet)items.Enlisted).Rollback();
#endif
                    }
                    else
                        ticket = VersionList.NewVersion(writeStamp);

                    if (theLock != null)
                        CommitLocker.Exit(theLock);
                }
                return true;
            }
            finally
            {
                if (_readTicket != oldReadTicket)
                    _readTicket = oldReadTicket;
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
#if USE_STD_HASHSET
                items.Enlisted.Any(HasChanges);
#else
                ((SimpleHashSet)items.Enlisted).HasChanges;
#endif

            WriteTicket ticket = null;
            if (!hasChanges)
            {
#if USE_STD_HASHSET
                foreach (var item in items.Enlisted)
                    item.Commit();
#else
                ((SimpleHashSet)items.Enlisted).CommitWoChanges();
#endif

                CloseTransaction();

                if (items.Fx != null)
                    items.Fx.Select(f => (Action)f.Commit).SafeRun();

                return true;
            }
            else if (CommitCheck(out ticket))
            {
                List<IShielded> trigger;
                // this must not be interrupted by a Thread.Abort!
                try { }
                finally
                {
#if USE_STD_HASHSET
                    trigger = new List<IShielded>();
                    foreach (var item in items.Enlisted)
                    {
                        if (item.HasChanges) trigger.Add(item);
                        item.Commit();
                    }
#else
                    trigger = ((SimpleHashSet)items.Enlisted).Commit();
#endif
                    ticket.Changes = trigger;
                    CloseTransaction();
                }

                (items.Fx != null ? items.Fx.Select(f => (Action)f.Commit) : null)
                    .SafeConcat(SubscriptionContext.PostCommit.Trigger(trigger))
                    .SafeRun();

                return true;
            }
            else
            {
                CloseTransaction();

                if (items.Fx != null)
                    items.Fx.Select(f => (Action)f.Rollback).SafeRun();

                return false;
            }
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
#if USE_STD_HASHSET
            foreach (var item in items.Enlisted)
                item.Rollback();
#else
            ((SimpleHashSet)items.Enlisted).Rollback();
#endif
            CloseTransaction();

            if (items.Fx != null)
                items.Fx.Select(f => (Action)f.Rollback).SafeRun();
        }

        private static void CloseTransaction()
        {
            try { }
            finally
            {
                VersionList.ReleaseReaderTicket(_readTicket);
                _readTicket = null;
                _localItems = null;
            }
        }

        #endregion
    }
}

