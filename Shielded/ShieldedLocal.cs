using System;

namespace Shielded
{
    /// <summary>
    /// Transactional local storage, lasts only during one transaction run and is
    /// reset when the transaction restarts or ends. Any value is still available in
    /// WhenCommitting and SyncSideEffect subscriptions, and in the context of
    /// a RunToCommit continuation. Throws if used outside of transactions.
    /// </summary>
    public class ShieldedLocal<T>
    {
        private TransactionalStorage<T> _storage = new TransactionalStorage<T>();

        /// <summary>
        /// Returns true if there is a value written in this local in this transaction.
        /// </summary>
        public bool HasValue
        {
            get
            {
                Shield.AssertInTransaction();
                return _storage.HasValue;
            }
        }

        /// <summary>
        /// Gets or sets the value contained in the local. If no value was set during
        /// this transaction, the getter throws.
        /// </summary>
        public T Value
        {
            get
            {
                Shield.AssertInTransaction();
                if (!_storage.HasValue)
                    throw new InvalidOperationException("ShieldedLocal has no value.");
                return _storage.Value;
            }
            set
            {
                Shield.AssertInTransaction();
                _storage.Value = value;
            }
        }

        /// <summary>
        /// Returns the current <see cref="Value"/>.
        /// </summary>
        public static implicit operator T(ShieldedLocal<T> local)
        {
            return local.Value;
        }

        /// <summary>
        /// Releases the storage, if any was used in the current transaction. Does not
        /// throw if it was not used.
        /// </summary>
        public void Release()
        {
            Shield.AssertInTransaction();
            _storage.Release();
        }
    }
}

