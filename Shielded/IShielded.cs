using System;

namespace Shielded
{
    internal interface IShielded
    {
        int PseudoHash { get; }

        bool HasChanges { get; }
        // this locks the implementor. All reads with >stamp are
        // spinwaited, all other threads' CanCommits() return false,
        // and only a Commit() or Rollback() release it.
        // it should lock only if it HasChanges!
        // Item1 is ManagedThreadId, the tuple is shared among items.
        bool CanCommit(Tuple<int, long> writeStamp);
        void Commit();
        void Rollback();
        void TrimCopies(long smallestOpenTransactionId);
    }

    internal interface ICommutableShielded : IShielded {}
}

