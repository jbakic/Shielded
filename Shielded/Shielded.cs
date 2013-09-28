using System;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded
{
    /// <summary>
    /// Makes your data thread safe. Works with structs, or simple value types,
    /// and the language does the necessary cloning. If T is a class, then only
    /// the reference itself is protected.
    /// </summary>
    public class Shielded<T> : ICommutableShielded
    {
        private class ValueKeeper
        {
            public long Version;
            public T Value;
            public ValueKeeper Older;
        }
        
        private ValueKeeper _current;
        // once negotiated, kept until commit or rollback
        private long _writerStamp;
        private LocalStorage<ValueKeeper> _locals = new LocalStorage<ValueKeeper>();

        public Shielded()
        {
            _current = new ValueKeeper();
        }

        public Shielded(T initial)
        {
            _current = new ValueKeeper();
            _current.Value = initial;
        }

        private void CheckLockAndEnlist()
        {
            var stamp = Shield.CurrentTransactionStartStamp;
            SpinWait.SpinUntil(() =>
            {
                var w = Interlocked.Read(ref _writerStamp);
                return w == 0 || w > stamp;
            });
            Shield.Enlist(this);
        }

        private ValueKeeper CurrentTransactionOldValue()
        {
            var point = _current;
            while (point != null && point.Version > Shield.CurrentTransactionStartStamp)
                point = point.Older;
            if (point == null)
                throw new ApplicationException("Critical error in Shielded<T> - lost data.");
            return point;
        }

        private void PrepareForWriting(bool prepareOld)
        {
            CheckLockAndEnlist();
            if (_current.Version > Shield.CurrentTransactionStartStamp)
                throw new TransException("Write collision.");
            if (!_locals.HasValue)
            {
                var v = new ValueKeeper();
                if (prepareOld)
                    v.Value = CurrentTransactionOldValue().Value;
                _locals.Value = v;
            }
        }

        /// <summary>
        /// If T is a value type, this returns a copy every time it's called!
        /// Works out of transaction also.
        /// </summary>
        public T Read
        {
            get
            {
                if (!Shield.IsInTransaction)
                    return _current.Value;

                CheckLockAndEnlist();
                if (!_locals.HasValue)
                    return CurrentTransactionOldValue().Value;
                else if (_current.Version > Shield.CurrentTransactionStartStamp)
                    throw new TransException("Writable read collision.");
                return _locals.Value.Value;
            }
        }

        public delegate void ModificationDelegate(ref T value);

        public void Modify(ModificationDelegate d)
        {
            PrepareForWriting(true);
            d(ref _locals.Value.Value);
        }

        /// <summary>
        /// Commutative, which means it won't conflict unless you read this shielded.
        /// </summary>
        public void Assign(T value)
        {
            Shield.EnlistCommute(() => {
                PrepareForWriting(false);
                _locals.Value.Value = value;
            }, this);
        }

        /// <summary>
        /// The action is performed just before commit, and reads the latest
        /// data. If it conflicts, only it is retried. If it succeeds,
        /// we (try to) commit with the same write stamp along with it.
        /// But, if you access this Shielded, it gets executed directly in this transaction.
        /// </summary>
        public void Commute(ModificationDelegate perform)
        {
            Shield.EnlistCommute(() => Modify(perform), this);
        }

        public static implicit operator T(Shielded<T> obj)
        {
            return obj.Read;
        }

        bool IShielded.HasChanges
        {
            get
            {
                return _locals.HasValue;
            }
        }
        
        bool IShielded.CanCommit(long writeStamp)
        {
            // these can be non-interlocked, since we're under lock.
            if (_writerStamp != 0)
                return false;
            else if (_current.Version <= Shield.CurrentTransactionStartStamp)
            {
                if (((IShielded)this).HasChanges)
                    _writerStamp = writeStamp;
                return true;
            }
            return false;
        }
        
        void IShielded.Commit(long? writeStamp)
        {
            if (((IShielded)this).HasChanges)
            {
                var newCurrent = _locals.Value;
                newCurrent.Older = _current;
                newCurrent.Version = writeStamp.Value;
                _current = newCurrent;
                if (Interlocked.CompareExchange(ref _writerStamp, 0, writeStamp.Value) != writeStamp.Value)
                    throw new ApplicationException("Commit from unexpected transaction.");
            }
            _locals.Value = null;
        }

        void IShielded.Rollback(long? writeStamp)
        {
            _locals.Value = null;
            if (writeStamp.HasValue)
                Interlocked.CompareExchange(ref _writerStamp, 0, writeStamp.Value);
        }
        
        void IShielded.TrimCopies(long smallestOpenTransactionId)
        {
            // NB the "smallest transaction" and others can freely read while
            // we're doing this.
            var point = _current;
            while (point != null && point.Version > smallestOpenTransactionId)
                point = point.Older;
            // point is the last accessible - his Older is not needed.
            if (point != null)
                point.Older = null;
            // if point were null above, data was lost, CurrentTransactionValue() might throw for some!
        }
    }
}

