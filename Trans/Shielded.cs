using System;
using System.Threading;
using System.Threading.Tasks;

namespace Trans
{
    /// <summary>
    /// Makes your data thread safe, provided you respect certain rules :)
    /// 
    /// Most importantly - the T should be a value type, or something implementing
    /// ICloneable, otherwise this is useless!
    /// 
    /// Also, Read does not enforce anything. For a value type T it will
    /// still be safe since you will get a copy, but for class types
    /// it depends on your good behaviour.
    /// </summary>
	public class Shielded<T> : IShielded
	{
        // TODO: Does not lock itself 

		private class ValueKeeper
		{
			public readonly long Version;
			public readonly T Value;
			public ValueKeeper Older;

            public ValueKeeper(long version, T val)
            {
                Version = version;
                Value = val;
            }
		}
		
		private ValueKeeper _current;
		
		private class CurrentItem
		{
			public long Version;
			public T Value;
            public bool Dirty = false;
		}
		
		private ThreadLocal<CurrentItem> _locals = new ThreadLocal<CurrentItem>();

		public Shielded(T initialValue)
		{
			_current = new ValueKeeper(0, initialValue);
		}
		
		private T CurrentTransactionOldValue()
		{
			var point = _current;
			if (Shield.IsInTransaction)
			{
				Shield.Enlist(this);
				while (point != null && point.Version > Shield.CurrentTransactionStartStamp)
					point = point.Older;
			}
			if (point == null)
				throw new ApplicationException("Critical error in Shielded<T> - lost data.");
            return point.Value;
		}

		private void PrepareForWriting()
        {
            if (!_locals.IsValueCreated || _locals.Value.Version != Shield.CurrentTransactionStartStamp)
            {
                var val = CurrentTransactionOldValue();
                if (val is ICloneable)
                    val = (T)((ICloneable)val).Clone();
                if (!_locals.IsValueCreated)
                    _locals.Value = new CurrentItem();
                _locals.Value.Value = val;
                _locals.Value.Version = Shield.CurrentTransactionStartStamp;
                _locals.Value.Dirty = true;
            }
            else if (!_locals.Value.Dirty)
            {
                if (_locals.Value is ICloneable)
                    _locals.Value.Value = (T)((ICloneable)_locals.Value.Value).Clone();
                _locals.Value.Dirty = true;
            }
            else if (_current.Version > Shield.CurrentTransactionStartStamp)
                throw new TransException("Write collision.");
		}

        /// <summary>
        /// Works out of transaction. You should not change anything, but this is not
        /// enforced.
        /// </summary>
		public T Read
        {
            get
            {
                if (!Shield.IsInTransaction)
                    return _current.Value;

                if (!_locals.IsValueCreated || _locals.Value.Version != Shield.CurrentTransactionStartStamp)
                {
                    var val = CurrentTransactionOldValue();
                    if (val == null || val is ValueType)
                        // caching a ValueType creates a copy, and when we return here, we make a copy
                        // of that copy... not neccessary.
                        return val;
                    else
                    {
                        if (!_locals.IsValueCreated)
                            _locals.Value = new CurrentItem();
                        _locals.Value.Value = val;
                        _locals.Value.Version = Shield.CurrentTransactionStartStamp;
                        _locals.Value.Dirty = false;
                    }
                }
                else if (_locals.Value.Dirty && _current.Version > Shield.CurrentTransactionStartStamp)
                    throw new TransException("Writable read collision.");
                return _locals.Value.Value;
            }
		}
		
		/// <summary>
		/// Getting and setting is assumed to change the value, so a copy is made. Commiting
        /// will require saving this, and all other ops to be "current".
		/// </summary>
		public T Write
		{
			get
			{
				PrepareForWriting();
				return _locals.Value.Value;
			}
			set
			{
				PrepareForWriting();
				_locals.Value.Value = value;
			}
		}
		
		bool IShielded.HasChanges
		{
			get
			{
				return _locals.IsValueCreated && _locals.Value.Version == Shield.CurrentTransactionStartStamp &&
                    _locals.Value.Dirty;
			}
		}
		
		bool IShielded.CanCommit(bool strict, long writeStamp)
		{
			return (!strict && !((IShielded)this).HasChanges) || _current.Version < Shield.CurrentTransactionStartStamp;
		}
		
		bool IShielded.Commit(long writeStamp)
        {
            if (((IShielded)this).HasChanges)
            {
                var newCurrent = new ValueKeeper(writeStamp, _locals.Value.Value)
                {
    				Older = _current
    			};
                _current = newCurrent;
                return true;
            }
            return false;
		}

        void IShielded.Rollback(long? writeStamp)
        {
            if (_locals.IsValueCreated && _locals.Value.Version == Shield.CurrentTransactionStartStamp)
                _locals.Value.Value = default(T);
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

