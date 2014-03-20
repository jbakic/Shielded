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
        private volatile Tuple<int, long> _writerStamp;
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
            if (!Shield.Enlist(this))
                return;

#if SERVER
            var stamp = Shield.CurrentTransactionStartStamp;
            var w = _writerStamp;
            if (w != null && w.Item2 <= stamp)
                lock (_locals)
                {
                    if ((w = _writerStamp) != null && w.Item2 <= stamp)
                        Monitor.Wait(_locals);
                }
#else
            SpinWait.SpinUntil(() =>
            {
                var w = _writerStamp;
                return w == null || w.Item2 > Shield.CurrentTransactionStartStamp;
            });
#endif
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
            Changed.Raise(this, EventArgs.Empty);
        }

        /// <summary>
        /// Writes the value into the Shielded. Not commutable, for performance reasons.
        /// </summary>
        public void Assign(T value)
        {
            PrepareForWriting(false);
            _locals.Value.Value = value;
            Changed.Raise(this, EventArgs.Empty);
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
            Shield.EnlistStrictCommute(() => {
                PrepareForWriting(true);
                perform(ref _locals.Value.Value);
            }, this);
            Changed.Raise(this, EventArgs.Empty);
        }

        /// <summary>
        /// For use by the ProxyGen, since I cannot create a ModificationDelegate closure in CodeDom..
        /// </summary>
        public void Commute(Action perform)
        {
            Shield.EnlistStrictCommute(perform, this);
            Changed.Raise(this, EventArgs.Empty);
        }

        /// <summary>
        /// Event raised after any change, and directly in the transaction that changed it.
        /// Subscriptions are transactional. In case of a commute, event is raised immediately
        /// after the commute is enlisted, and your handler can easily cause commutes to
        /// degenerate.
        /// </summary>
        public readonly ShieldedEvent<EventArgs> Changed = new ShieldedEvent<EventArgs>();

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
        
        bool IShielded.CanCommit(Tuple<int, long> writeStamp)
        {
            if (_writerStamp != null)
                return false;
            else if (_current.Version <= Shield.CurrentTransactionStartStamp)
            {
                if (_locals.HasValue)
                    _writerStamp = writeStamp;
                return true;
            }
            return false;
        }
        
        void IShielded.Commit()
        {
            if (!_locals.HasValue)
                return;
            var newCurrent = _locals.Value;
            newCurrent.Older = _current;
            newCurrent.Version = _writerStamp.Item2;
            _current = newCurrent;
            _locals.Value = null;
#if SERVER
            lock (_locals)
            {
                _writerStamp = null;
                Monitor.PulseAll(_locals);
            }
#else
            _writerStamp = null;
#endif
        }

        void IShielded.Rollback()
        {
            if (!_locals.HasValue)
                return;
            _locals.Value = null;
            var ws = _writerStamp;
            if (ws != null && ws.Item1 == Thread.CurrentThread.ManagedThreadId)
            {
#if SERVER
                lock (_locals)
                {
                    _writerStamp = null;
                    Monitor.PulseAll(_locals);
                }
#else
                _writerStamp = null;
#endif
            }
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

