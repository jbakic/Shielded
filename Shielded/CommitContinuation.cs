using System;
using System.Threading;

namespace Shielded
{
    /// <summary>
    /// An object used to commit a transaction at a later time, or from another
    /// thread. Returned by <see cref="Shield.RunToCommit"/>. The transaction has
    /// been checked, and is OK to commit. This class is thread-safe - it makes
    /// sure only one thread can initiate a commit, but any number of threads
    /// may try a rollback in parallel.
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
        /// Commit the transaction held in this continuation. Throws if it's already completing/ed.
        /// </summary>
        public virtual void Commit()
        {
            if (!TryCommit())
                throw new InvalidOperationException("Transaction already completing or completed.");
        }

        /// <summary>
        /// Try to commit the transaction held in this continuation, returning true if successful.
        /// It returns true only if this call actually did the full commit.
        /// </summary>
        public abstract bool TryCommit();

        /// <summary>
        /// Roll back the transaction held in this continuation. Throws if already completing/ed.
        /// </summary>
        public virtual void Rollback()
        {
            if (!TryRollback())
                throw new InvalidOperationException("Transaction already completing or completed.");
        }

        /// <summary>
        /// Try to roll back the transaction held in this continuation, returning true if successful.
        /// Returns true only if this call actually performs the rollback.
        /// </summary>
        public abstract bool TryRollback();

        /// <summary>
        /// Meta-info on the fields affected by this transaction.
        /// </summary>
        public abstract TransactionField[] Fields
        {
            get;
        }

        /// <summary>
        /// Run the action inside the transaction context, with information on the
        /// transaction's access pattern given in its argument. Access is limited to
        /// exactly what the main transaction already did. Throws if the continuation
        /// has completed, dangerous if it is completing.
        /// </summary>
        public void InContext(Action<TransactionField[]> act)
        {
            InContext(() => act(Fields));
        }

        /// <summary>
        /// Run the action inside the transaction context. Access is limited to
        /// exactly what the main transaction already did. Throws if the continuation
        /// has completed, dangerous if it is completing.
        /// </summary>
        public abstract void InContext(Action act);

        private Timer _timer;

        internal void StartTimer(int ms)
        {
            _timer = new Timer(_ => Dispose(), null, ms, Timeout.Infinite);
        }

        /// <summary>
        /// If not <see cref="Completed"/>, calls <see cref="Rollback"/>.
        /// </summary>
        public void Dispose()
        {
            if (_timer != null)
            {
                _timer.Dispose();
                _timer = null;
            }
            if (!Completed)
                TryRollback();
        }
    }
}

