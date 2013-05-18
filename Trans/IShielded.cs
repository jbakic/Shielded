using System;

namespace Trans
{
    internal interface IShielded
    {
        bool HasChanges { get; }
        // this locks the implementor. All reads with >stamp are
        // spinwaited, all other threads' CanCommits() return false,
        // and only a Commit() or Rollback() (with the same stamp) release it.
        bool CanCommit(bool strict, long writeStamp);
        bool Commit(long writeStamp);
        void Rollback(long? writeStamp = null);
        void TrimCopies(long smallestOpenTransactionId);
    }
}

