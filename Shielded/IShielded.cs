using System;

namespace Shielded
{
    internal interface IShielded
    {
        bool HasChanges { get; }
        // this locks the implementor. All reads with >stamp are
        // spinwaited, all other threads' CanCommits() return false,
        // and only a Commit() or Rollback() (with the same stamp) release it.
        // it should lock only if it HasChanges!
        bool CanCommit(long writeStamp);
        // if no changes were made anywhere, this is called directly with a null stamp.
        bool Commit(long? writeStamp);
        void Rollback(long? writeStamp = null);
        void TrimCopies(long smallestOpenTransactionId);
    }

    internal interface ICommutableShielded : IShielded {}
}

