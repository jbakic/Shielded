using System;
using System.Threading;

namespace Shielded
{
    /// <summary>
    /// Makes your data thread safe. Works with structs, or simple value types,
    /// and the language does the necessary cloning. If T is a class, then only
    /// the reference itself is protected.
    /// </summary>
    public class Shielded<T> : IShielded
    {
        private class ValueKeeper
        {
            public long Version;
            public T Value;
            public ValueKeeper Older;
        }
        
        private ValueKeeper _current;
        // once negotiated, kept until commit or rollback
        private volatile WriteStamp _writerStamp;
        private readonly TransactionalStorage<ValueKeeper> _locals = new TransactionalStorage<ValueKeeper>();
        private readonly object _owner;

        /// <summary>
        /// Constructs a new Shielded container, containing default value of type T.
        /// </summary>
        /// <param name="owner">If this is given, then in WhenCommitting subscriptions
        /// this shielded will report its owner instead of itself.</param>
        public Shielded(object owner = null)
        {
            _current = new ValueKeeper();
            _owner = owner ?? this;
        }

        /// <summary>
        /// Constructs a new Shielded container, containing the given initial value.
        /// </summary>
        /// <param name="initial">Initial value to contain.</param>
        /// <param name="owner">If this is given, then in WhenCommitting subscriptions
        /// this shielded will report its owner instead of itself.</param>
        public Shielded(T initial, object owner = null)
        {
            _current = new ValueKeeper();
            _current.Value = initial;
            _owner = owner ?? this;
        }

        /// <summary>
        /// Enlists the field in the current transaction and, if this is the first
        /// access, checks the write lock. Will wait (using StampLocker) if the write
        /// stamp &lt;= <see cref="Shield.ReadStamp"/>, until write lock is released.
        /// Since write stamps are increasing, this is likely to happen only at the
        /// beginning of transactions.
        /// </summary>
        private void CheckLockAndEnlist(bool write)
        {
            // if already enlisted, no need to check lock.
            if (!Shield.Enlist(this, _locals.HasValue, write))
                return;

            var ws = _writerStamp;
            if (ws != null && ws.Locked && ws.Version <= Shield.ReadStamp)
                ws.Wait();
        }

        private ValueKeeper CurrentTransactionOldValue()
        {
            var point = _current;
            while (point.Version > Shield.ReadStamp)
                point = point.Older;
            return point;
        }

        /// <summary>
        /// Gets the value that this Shielded contained at transaction opening. During
        /// a transaction, this is constant. See also <see cref="Shield.ReadOldState"/>.
        /// </summary>
        public T GetOldValue()
        {
            CheckLockAndEnlist(false);
            return CurrentTransactionOldValue().Value;
        }

        private ValueKeeper PrepareForWriting(bool prepareOld)
        {
            CheckLockAndEnlist(true);
            if (_current.Version > Shield.ReadStamp)
                throw new TransException("Write collision.");
            if (_locals.HasValue)
                return _locals.Value;

            var v = new ValueKeeper();
            if (prepareOld)
                v.Value = CurrentTransactionOldValue().Value;
            _locals.Value = v;
            return v;
        }

        /// <summary>
        /// Reads or writes into the content of the field. Reading can be
        /// done out of transaction, but writes must be inside.
        /// </summary>
        public T Value
        {
            get
            {
                if (!Shield.IsInTransaction)
                    return _current.Value;

                CheckLockAndEnlist(false);
                if (!_locals.HasValue || Shield.ReadingOldState)
                    return CurrentTransactionOldValue().Value;
                else if (_current.Version > Shield.ReadStamp)
                    throw new TransException("Writable read collision.");
                return _locals.Value.Value;
            }
            set
            {
                PrepareForWriting(false).Value = value;
                RaiseChanged();
            }
        }

        /// <summary>
        /// Delegate type used for modifications, i.e. read and write operations.
        /// It has the advantage of working directly on the internal, thread-local
        /// storage copy, to which it gets a reference. This is more efficient if
        /// the type T is a big value-type.
        /// </summary>
        public delegate void ModificationDelegate(ref T value);

        /// <summary>
        /// Modifies the content of the field, i.e. read and write operation.
        /// It has the advantage of working directly on the internal, thread-local
        /// storage copy, to which it gets a reference. This is more efficient if
        /// the type T is a big value-type.
        /// </summary>
        public void Modify(ModificationDelegate d)
        {
            d(ref PrepareForWriting(true).Value);
            RaiseChanged();
        }

        /// <summary>
        /// The action is performed just before commit, and reads the latest
        /// data. If it conflicts, only it is retried. If it succeeds,
        /// we (try to) commit with the same write stamp along with it.
        /// But, if you access this Shielded, it gets executed directly in this transaction.
        /// The Changed event is raised only when the commute is enlisted, and not
        /// when (and every time, given possible repetitions..) it executes.
        /// </summary>
        public void Commute(ModificationDelegate perform)
        {
            Shield.EnlistStrictCommute(
                () => perform(ref PrepareForWriting(true).Value), this);
            RaiseChanged();
        }

        /// <summary>
        /// For use by the ProxyGen, since the users of it know only about the base type used
        /// to generate the proxy. The struct used internally is not exposed, and so users
        /// of proxy classes could not write a ModificationDelegate which works on an argument
        /// whose type is that hidden struct.
        /// </summary>
        public void Commute(Action perform)
        {
            Shield.EnlistStrictCommute(perform, this);
            RaiseChanged();
        }

        /// <summary>
        /// Event raised after any change, and directly in the transaction that changed it.
        /// Subscriptions are transactional. In case of a commute, event is raised immediately
        /// after the commute is enlisted, and your handler can easily cause commutes to
        /// degenerate.
        /// </summary>
        public ShieldedEvent<EventArgs> Changed
        {
            get
            {
                var ev = _changed;
                if (ev == null)
                {
                    var newObj = new ShieldedEvent<EventArgs>();
                    ev = Interlocked.CompareExchange(ref _changed, newObj, null) ?? newObj;
                }
                return ev;
            }
        }
        private ShieldedEvent<EventArgs> _changed;

        private void RaiseChanged()
        {
            var ev = _changed;
            if (ev != null)
                ev.Raise(this, EventArgs.Empty);
        }

        /// <summary>
        /// Returns the current <see cref="Value"/>.
        /// </summary>
        public static implicit operator T(Shielded<T> obj)
        {
            return obj.Value;
        }

        bool IShielded.HasChanges
        {
            get
            {
                return _locals.HasValue;
            }
        }

        object IShielded.Owner
        {
            get
            {
                return _owner;
            }
        }

        bool IShielded.CanCommit(WriteStamp writeStamp)
        {
            var res = _writerStamp == null &&
                _current.Version <= Shield.ReadStamp;
            if (res && _locals.HasValue)
                _writerStamp = writeStamp;
            return res;
        }
        
        void IShielded.Commit()
        {
            if (!_locals.HasValue)
                return;
            var newCurrent = _locals.Value;
            newCurrent.Older = _current;
            newCurrent.Version = _writerStamp.Version.Value;
            _current = newCurrent;
            _writerStamp = null;
            _locals.Release();
        }

        void IShielded.Rollback()
        {
            if (!_locals.HasValue)
                return;
            var ws = _writerStamp;
            if (ws != null && ws.Locker == Shield.Context)
                _writerStamp = null;
            _locals.Release();
        }
        
        void IShielded.TrimCopies(long smallestOpenTransactionId)
        {
            // NB the "smallest transaction" and others can freely read while
            // we're doing this.
            var point = _current;
            while (point.Version > smallestOpenTransactionId)
                point = point.Older;
            // point is the last accessible - his Older is not needed.
            point.Older = null;
        }
    }
}

