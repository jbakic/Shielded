using System;
using System.Threading;

namespace Trans
{
    /// <summary>
    /// Shielded reference. Very weak - only the reference itself is protected, not the
    /// data inside the referenced object!
    /// </summary>
    public class ShieldedRef<T> : IShielded where T : class
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
        private ThreadLocal<ValueKeeper> _locals = new ThreadLocal<ValueKeeper>(() => new ValueKeeper());

        public ShieldedRef()
        {
            _current = new ValueKeeper();
        }

        public ShieldedRef(T initial)
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
            CheckLockAndEnlist();

            var point = _current;
            while (point != null && point.Version > Shield.CurrentTransactionStartStamp)
                point = point.Older;
            if (point == null)
                throw new ApplicationException("Critical error in Shielded<T> - lost data.");
            return point;
        }

        private bool IsLocalPrepared()
        {
            return _locals.IsValueCreated && _locals.Value != null;
        }

        private void PrepareForWriting()
        {
            if (_current.Version > Shield.CurrentTransactionStartStamp)
                throw new TransException("Write collision.");
            if (!IsLocalPrepared())
            {
                _locals.Value = new ValueKeeper();
                CheckLockAndEnlist();
            }
        }

        public T Read
        {
            get
            {
                if (!Shield.IsInTransaction)
                    return _current.Value;

                if (!IsLocalPrepared())
                    return CurrentTransactionOldValue().Value;
                else if (_current.Version > Shield.CurrentTransactionStartStamp)
                    throw new TransException("Writable read collision.");
                return _locals.Value.Value;
            }
        }

        public static implicit operator T(ShieldedRef<T> s)
        {
            return s.Read;
        }

        public void Assign(T val)
        {
            PrepareForWriting();
            _locals.Value.Value = val;
        }

        bool IShielded.HasChanges
        {
            get
            {
                return IsLocalPrepared();
            }
        }
        
        bool IShielded.CanCommit(bool strict, long writeStamp)
        {
            if (!strict && !((IShielded)this).HasChanges)
                return true;
            else if (Interlocked.Read(ref _writerStamp) != 0)
                return false;
            else if (_current.Version < Shield.CurrentTransactionStartStamp)
            {
                if (((IShielded)this).HasChanges)
                    Interlocked.Exchange(ref _writerStamp, writeStamp);
                return true;
            }
            return false;
        }
        
        bool IShielded.Commit(long? writeStamp)
        {
            if (((IShielded)this).HasChanges)
            {
                if (Interlocked.Read(ref _writerStamp) != writeStamp.Value)
                    throw new ApplicationException("Commit received from unexpected transaction.");
                var newCurrent = _locals.Value;
                newCurrent.Older = _current;
                newCurrent.Version = writeStamp.Value;
                _current = newCurrent;
                _locals.Value = null;
                Interlocked.Exchange(ref _writerStamp, 0);
                return true;
            }
            _locals.Value = null;
            return false;
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
            if (point != null) point.Older = null;
            // if point were null above, data was lost, CurrentTransactionValue() might throw for some!
        }
    }
}

