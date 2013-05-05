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

        private static HashSet<long> _openTransactions = new HashSet<long>();
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
            _transactionItems.AddOrUpdate(
                CurrentTransactionStartStamp,
                new HashSet<IShielded>(new [] { item }),
                (threadId, old) => { old.Add(item); return old; });
        }

        public static void SideEffect(Action fx, Action rollbackFx = null)
        {
            Enlist(new SideEffect(fx, rollbackFx));
        }

        public static void InTransaction(Action act)
        {
            bool repeat;
            do
            {
                repeat = false;
                bool createdTransaction = false;
                if (_currentTransactionStartStamp == null)
                {
                    // for stable reading, numbers cannot be obtained while transaction commit is going on.
                    lock (_commitLock)
                    {
                        createdTransaction = true;
                        _currentTransactionStartStamp = Interlocked.Increment(ref _lastStamp);
                        _openTransactions.Add(_currentTransactionStartStamp.Value);
                    }
                }

                try
                {
                    act();

                    if (createdTransaction)
                        DoCommit();
                }
                catch (TransException ex)
                {
                    if (createdTransaction)
                    {
                        DoRollback();
                        repeat = !(ex is NoRepeatTransException);
                    }
                    else
                        throw;
                }
                catch (Exception)
                {
                    if (createdTransaction)
                        DoRollback();
                    throw;
                }
            } while (repeat);
        }

        private static object _commitLock = new object();

        private static void DoCommit()
        {
            // if there are items, they must all commit.
            HashSet<IShielded> items;
            _transactionItems.TryGetValue(CurrentTransactionStartStamp, out items);
            List<IShielded> copies = new List<IShielded>();
            long minTransaction;

            lock (_commitLock)
            {
                bool isStrict = items.Any(s => s.HasChanges);
                foreach (var item in items)
                    if (!item.CanCommit(isStrict))
                        throw new TransException("Commit collision.");

                var writeStamp = Interlocked.Increment(ref _lastStamp);
                foreach (var item in items)
                    if (item.Commit(writeStamp))
                        copies.Add(item);
                _transactionItems.TryRemove(CurrentTransactionStartStamp, out items);
                _openTransactions.Remove(CurrentTransactionStartStamp);
                minTransaction = _openTransactions.Any() ? _openTransactions.Min() :
                    Interlocked.Read(ref _lastStamp);
//#if DEBUG
//                Console.WriteLine("Stamp {0} commited with stamp {1}. Open count: {2}, item sets: {3}.",
//                    _currentTransactionStartStamp.Value, writeStamp, _openTransactions.Count,
//                    _transactionItems.Count);
//#endif
            }
            RegisterCopies(CurrentTransactionStartStamp, copies);
            TrimCopies(minTransaction);
            _currentTransactionStartStamp = null;
        }

        private static void DoRollback()
        {
            long minTransaction;
            lock (_commitLock)
            {
                HashSet<IShielded> items;
                _transactionItems.TryRemove(_currentTransactionStartStamp.Value, out items);
                _openTransactions.Remove(_currentTransactionStartStamp.Value);
                foreach (var item in items)
                    item.Rollback();

                minTransaction = _openTransactions.Any() ? _openTransactions.Min() :
                    Interlocked.Read(ref _lastStamp);
            }
            TrimCopies(minTransaction);
            _currentTransactionStartStamp = null;
        }
    }
}

