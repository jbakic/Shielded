using System;

namespace Shielded
{
    /// <summary>
    /// Contains information on the thread that has locked an object.
    /// </summary>
    internal class WriteStamp
    {
        /// <summary>
        /// ManagedThreadId of the locking thread. Useful to make sure that
        /// a thread, on rollback, releases only those locks it made.
        /// </summary>
        public readonly int ThreadId;

        /// <summary>
        /// The version of the data that is about to be written into the
        /// locked object. All threads which should be able to read this
        /// version (i.e. their <see cref="Shield.ReadStamp"/> is greater
        /// or equal to this value) must wait for the lock to be released.
        /// If the Version is null, then this stamp is not yet confirmed,
        /// and readers may proceed (since when and if it is confirmed, the
        /// Version will be at least 1 bigger than the current Shield._lastStamp,
        /// which means no current reader can have a bigger or equal stamp).
        /// </summary>
        public long? Version;

        public WriteStamp(int threadId)
        {
            ThreadId = threadId;
        }
    }
}

