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
        /// True if <see cref="Commit"/> or <see cref="Rollback"/> have been called.
        /// </summary>
        public bool Completed
        {
            get;
            protected set;
        }

        /// <summary>
        /// Commit the transaction held in this continuation.
        /// Warning: WhenCommitting subscriptions are executed first - any excptions
        /// that they throw will bubble out of this method, and cause the commit to fail!
        /// </summary>
        public abstract void Commit();

        /// <summary>
        /// Roll back the transaction held in this continuation.
        /// </summary>
        public abstract void Rollback();

        /// <summary>
        /// Runs the action inside the transaction context, with information on the
        /// transaction's access pattern given in its argument.
        /// </summary>
        public abstract void InContext(Action<TransactionField[]> act);

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
                Rollback();
        }
    }
}

