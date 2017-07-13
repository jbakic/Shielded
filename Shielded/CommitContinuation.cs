using System;
using System.Threading;

namespace Shielded
{
    /// <summary>
    /// An object used to commit a transaction at a later time, or from another
    /// thread. Returned by <see cref="Shield.RunToCommit"/>. The transaction has
    /// been checked, and is OK to commit. This class is thread-safe, but may throw
    /// <see cref="ContinuationCompletedException"/> if another thread has completed it.
    /// </summary>
    public abstract class CommitContinuation : IDisposable
    {
        /// <summary>
        /// True if <see cref="Commit"/> or <see cref="Rollback"/> have completed.
        /// Volatile and set last, so after this becomes true, <see cref="Committed"/>
        /// reliably indicates the outcome of the transaction.
        /// </summary>
        public bool Completed
        {
            get { return _completed; }
            protected set { _completed = value; }
        }
        private volatile bool _completed;

        /// <summary>
        /// True if this continuation has successfully committed.
        /// </summary>
        public bool Committed
        {
            get;
            protected set;
        }

        /// <summary>
        /// Commit the transaction held in this continuation. Throws if it's already completed.
        /// </summary>
        /// <exception cref="ContinuationCompletedException"/>
        public virtual void Commit()
        {
            if (!TryCommit())
                throw new ContinuationCompletedException();
        }

        /// <summary>
        /// Try to commit the transaction held in this continuation, returning true if successful.
        /// It returns true only if this call actually did the full commit.
        /// </summary>
        public abstract bool TryCommit();

        /// <summary>
        /// Roll back the transaction held in this continuation. Throws if already completed.
        /// </summary>
        /// <exception cref="ContinuationCompletedException"/>
        public virtual void Rollback()
        {
            if (!TryRollback())
                throw new ContinuationCompletedException();
        }

        /// <summary>
        /// Try to roll back the transaction held in this continuation, returning true if successful.
        /// Returns true only if this call actually performs the rollback.
        /// </summary>
        public abstract bool TryRollback();

        /// <summary>
        /// Meta-info on the fields affected by this transaction.
        /// </summary>
        /// <exception cref="ContinuationCompletedException"/>
        public abstract TransactionField[] Fields
        {
            get;
        }

        /// <summary>
        /// Run the action inside the transaction context, with information on the
        /// transaction's access pattern given in its argument. Access is limited to
        /// exactly what the main transaction already did. Throws if the continuation
        /// has completed. Synchronizes with any other threads concurrently
        /// trying to commit or rollback.
        /// </summary>
        /// <exception cref="ContinuationCompletedException"/>
        public void InContext(Action<TransactionField[]> act)
        {
            InContext(() => act(Fields));
        }

        /// <summary>
        /// Try to run the action inside the transaction context, with information on the
        /// transaction's access pattern given in its argument. Access is limited to
        /// exactly what the main transaction already did. Synchronizes with any other
        /// threads concurrently trying to commit or rollback.
        /// </summary>
        public bool TryInContext(Action<TransactionField[]> act)
        {
            return TryInContext(() => act(Fields));
        }

        /// <summary>
        /// Run the action inside the transaction context. Access is limited to
        /// exactly what the main transaction already did. Throws if the continuation
        /// has completed. Synchronizes with any other threads concurrently
        /// trying to commit or rollback.
        /// </summary>
        /// <exception cref="ContinuationCompletedException"/>
        public virtual void InContext(Action act)
        {
            if (!TryInContext(act))
                throw new ContinuationCompletedException();
        }

        /// <summary>
        /// Try to run the action inside the transaction context. Access is limited to
        /// exactly what the main transaction already did. Synchronizes with any other
        /// threads concurrently trying to commit or rollback.
        /// </summary>
        public abstract bool TryInContext(Action act);

        private Timer _timer;

        internal virtual void StartTimer(int ms)
        {
            _timer = new Timer(_ => Dispose(), null, ms, Timeout.Infinite);
        }

        /// <summary>
        /// If not <see cref="Completed"/>, calls <see cref="TryRollback"/>.
        /// </summary>
        public void Dispose()
        {
            var timer = _timer;
            if (timer != null)
            {
                timer.Dispose();
                _timer = null;
            }
            if (!Completed)
                TryRollback();
        }
    }
}

