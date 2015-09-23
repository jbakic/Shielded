using System;

namespace Shielded
{
    /// <summary>
    /// Contains information on the thread that has locked an object.
    /// </summary>
    internal class WriteStamp
    {
        /// <summary>
        /// Reference to the locking transaction. Useful to make sure that
        /// a thread, on rollback, releases only those locks it made.
        /// </summary>
        public readonly TransactionContext Locker;

        /// <summary>
        /// The version of the data that is about to be written into the
        /// locked object. All threads which should be able to read this
        /// version (i.e. their <see cref="Shield.ReadStamp"/> is greater
        /// or equal to this value) must wait for the lock to be released.
        /// If the value is null, then this lock is still being checked and
        /// the reader may proceed - when/if it passes check, it will certainly
        /// get a higher value than current reader's read stamp.
        /// </summary>
        public long? Version;

        public WriteStamp(TransactionContext locker)
        {
            Locker = locker;
        }
    }
}

