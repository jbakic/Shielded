using System;

namespace Shielded
{
    /// <summary>
    /// Transactional local storage, lasts only during one transaction run and is
    /// reset when the transaction restarts or ends. Any value is still available in
    /// WhenCommitting and SyncSideEffect subscriptions, and in the context of
    /// a RunToCommit continuation.
    /// </summary>
    public class ShieldedLocal<T>
    {
        private TransactionalStorage<T> _storage = new TransactionalStorage<T>();

        public bool HasValue
        {
            get
            {
                Shield.AssertInTransaction();
                return _storage.HasValue;
            }
        }

        public T Value
        {
            get
            {
                Shield.AssertInTransaction();
                return _storage.HasValue ? _storage.Value : default(T);
            }
            set
            {
                Shield.AssertInTransaction();
                _storage.Value = value;
            }
        }

        public void Release()
        {
            Shield.AssertInTransaction();
            _storage.Release();
        }
    }
}

