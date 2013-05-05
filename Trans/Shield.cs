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

        // happens during commit only, so under lock.
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

        // during commit only
        private static void TrimCopies(long minTransactionNo)
        {
            lock (_copiesByVersion)
            {
                int toDelete = 0;
                for (; toDelete < _copiesByVersion.Count &&
                     _copiesByVersion[toDelete].Item1 < minTransactionNo; toDelete++)
                    ;
                toDelete--;
                if (toDelete > 0)
                {
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
            HashSet<IShielded> items;
            // reading the current stamp throws ...
            _transactionItems.TryGetValue(CurrentTransactionStartStamp, out items);
            items.Add(item);
        }

        public static void SideEffect(Action fx, Action rollbackFx = null)
        {
            Enlist(new SideEffect(fx, rollbackFx));
        }

        public static void InTransaction(Action act)
        {
            if (_currentTransactionStartStamp.HasValue)
                act();

            bool repeat;
            do
            {
                repeat = false;
                // for stable reading, numbers cannot be obtained while transaction commit is going on.
                lock (_commitLock)
                {
                    _currentTransactionStartStamp = Interlocked.Increment(ref _lastStamp);
                    if (!_transactionItems.TryAdd(_currentTransactionStartStamp.Value,
                            new HashSet<IShielded>()))
                        throw new ApplicationException();
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
            HashSet<IShielded> items;
            _transactionItems.TryGetValue(CurrentTransactionStartStamp, out items);
            List<IShielded> copies = new List<IShielded>();
            long minTransaction;

            bool isStrict = items.Any(s => s.HasChanges);
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

            var oldStamp = _currentTransactionStartStamp.Value;
            _transactionItems.TryRemove(CurrentTransactionStartStamp, out items);
            if (commit)
            {
                // do the commit. out of global lock! and, non side-effects first.
                foreach (var item in nonFx)
                    if (item.Commit(writeStamp))
                        copies.Add(item);

                _currentTransactionStartStamp = null;

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

            var keys = _transactionItems.Keys;
            minTransaction = keys.Any() ? keys.Min() : Interlocked.Read(ref _lastStamp);
            RegisterCopies(oldStamp, copies);
            TrimCopies(minTransaction);

            return commit;
        }

        private static void DoRollback()
        {
            long minTransaction;
            HashSet<IShielded> items;
            _transactionItems.TryRemove(_currentTransactionStartStamp.Value, out items);
            foreach (var item in items.Where(i => !(i is SideEffect)))
                item.Rollback();
            _currentTransactionStartStamp = null;

            foreach (var item in items.OfType<SideEffect>())
                item.Rollback(null);

            var keys = _transactionItems.Keys;
            minTransaction = keys.Any() ? keys.Min() : Interlocked.Read(ref _lastStamp);
            TrimCopies(minTransaction);
        }
    }
}

