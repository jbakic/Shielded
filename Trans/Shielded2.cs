using System;
using System.Threading;
using System.Threading.Tasks;

namespace Trans
{
    /// <summary>
    /// Makes your data thread safe :)
    /// 
    /// This one works with structs, which means C# will be doing the cloning.
    /// </summary>
	public class Shielded2<T> : IShielded where T : struct
	{
		private class ValueKeeper
		{
			public long Version;
			public T Value;
			public ValueKeeper Older;
		}
		
		private ValueKeeper _current;
        // once negotiated, kept until commit or rollback
        private long _currentWriter;
		private ThreadLocal<ValueKeeper> _locals = new ThreadLocal<ValueKeeper>(() => new ValueKeeper());

		public Shielded2()
		{
			_current = new ValueKeeper();
		}

        public Shielded2(T initial)
        {
            _current = new ValueKeeper();
            _current.Value = initial;
        }

		private ValueKeeper CurrentTransactionOldValue()
		{
            Shield2.Enlist(this);

			var point = _current;
			while (point != null && point.Version > Shield2.CurrentTransactionStartStamp)
				point = point.Older;
			if (point == null)
				throw new ApplicationException("Critical error in Shielded2<T> - lost data.");
            return point;
		}

        private bool IsLocalPrepared()
        {
            return _locals.IsValueCreated && _locals.Value != null &&
                _locals.Value.Version == Shield2.CurrentTransactionStartStamp;
        }

		private void PrepareForWriting()
        {
            // this if test creates it if it did not exist!
            if (!IsLocalPrepared())
            {
                if (_locals.Value == null)
                    _locals.Value = new ValueKeeper();
                _locals.Value.Value = CurrentTransactionOldValue().Value;
                _locals.Value.Version = Shield2.CurrentTransactionStartStamp;
            }
            else if (_current.Version > Shield2.CurrentTransactionStartStamp)
                throw new TransException("Write collision.");
		}

        /// <summary>
        /// Since T is a value type, this returns a copy every time it's called!
        /// Works out of transaction also.
        /// </summary>
		public T Read
        {
            get
            {
                if (!Shield2.IsInTransaction)
                    return _current.Value;

                if (!IsLocalPrepared())
                    return CurrentTransactionOldValue().Value;
                else if (_current.Version > Shield2.CurrentTransactionStartStamp)
                    throw new TransException("Writable read collision.");
                return _locals.Value.Value;
            }
		}

        public delegate void ModificationDelegate(ref T value);

        public void Modify(ModificationDelegate d)
        {
            PrepareForWriting();
            d(ref _locals.Value.Value);
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
			return (!strict && !((IShielded)this).HasChanges) || _current.Version < Shield2.CurrentTransactionStartStamp;
		}
		
		bool IShielded.Commit(long writeStamp)
        {
            if (((IShielded)this).HasChanges)
            {
                var newCurrent = _locals.Value;
                newCurrent.Older = _current;
                newCurrent.Version = writeStamp;
                _current = newCurrent;
                _locals.Value = null;
                return true;
            }
            return false;
		}

        void IShielded.Rollback()
        {
            if (_locals.IsValueCreated && _locals.Value.Version == Shield2.CurrentTransactionStartStamp)
                _locals.Value = null;
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

