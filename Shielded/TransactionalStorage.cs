using System;
using System.Threading;
using System.Collections.Generic;

namespace Shielded
{
    /// <summary>
    /// Transactional storage, but very specialized, and thus internal.
    /// </summary>
    internal class TransactionalStorage<T> where T : class
    {
        // These two are faster, immediate storage, which can be used by one transaction only.
        // If there is more than one, transactional storage is used by the others.
        private TransactionContext _holderContext;
        private T _heldValue;

        /// <summary>
        /// Returns <c>true</c> if current transaction has something inside this storage.
        /// </summary>
        public bool HasValue
        {
            get
            {
                var ctx = Shield.Context;
                return ctx == _holderContext ||
                    (ctx.Storage != null && ctx.Storage.ContainsKey(this));
            }
        }

        /// <summary>
        /// Gets or sets the value in the current transaction. Use <see cref="Release"/>
        /// to release the storage.
        /// </summary>
        public T Value
        {
            get
            {
                var ctx = Shield.Context;
                return _holderContext == ctx ?
                    _heldValue : (T)ctx.Storage[this];
            }
            set
            {
                var ctx = Shield.Context;
                var holder = Interlocked.CompareExchange(ref _holderContext, ctx, null);
                if (holder == ctx || holder == null)
                {
                    _heldValue = value;
                }
                else
                {
                    if (ctx.Storage == null)
                        ctx.Storage = new Dictionary<object, object>();
                    ctx.Storage[this] = value;
                }
            }
        }

        /// <summary>
        /// Release the storage of this object in the current transaction.
        /// </summary>
        public void Release()
        {
            var ctx = Shield.Context;
            if (_holderContext == ctx)
            {
                _heldValue = null;
                _holderContext = null;
            }
            else if (ctx.Storage != null)
            {
                ctx.Storage.Remove(this);
            }
        }
    }
}

