using System;
using System.Threading;
using System.Collections.Generic;

namespace Shielded
{
    /// <summary>
    /// Transactional storage - value inside depends on the transaction accessing it. Works
    /// only inside transactions. It is simple, using direct local fields when one transaction
    /// is using it, and resorting to the transactional storage otherwise.
    /// </summary>
    public class TransactionalStorage<T> where T : class
    {
        // These two are faster, immediate storage, which can be used by one transaction only.
        // If there is more than one, transactional storage is used by the others.
        private TransactionContext _holderContext;
        private T _heldValue;

        static TransactionContext GetContext()
        {
            var ctx = Shield.Context;
            if (ctx == null)
                throw new InvalidOperationException("Operation allowed only inside transactions.");
            return ctx;
        }

        /// <summary>
        /// Returns <c>true</c> if current transaction has something inside this storage.
        /// </summary>
        public bool HasValue
        {
            get
            {
                var ctx = GetContext();
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
                var ctx = GetContext();
                return _holderContext == ctx ?
                    _heldValue : (T)ctx.Storage[this];
            }
            set
            {
                var ctx = GetContext();
                var holder = Interlocked.CompareExchange(ref _holderContext, ctx, null);
                if (holder == ctx || holder == null)
                {
                    _heldValue = value;
                    // A bugfix for a bug that never was. -- If we're just now taking over
                    // local fields, then what if we already had something in the dictionary?
                    // Let's remove it then. The bug, however, never happened, because no user
                    // of this class makes two consecutive writes. They put something in,
                    // then null, and only then something else. But it's best to be safe.
                    if (holder == null && ctx.Storage != null)
                        ctx.Storage.Remove(this);
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
            var ctx = GetContext();
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

