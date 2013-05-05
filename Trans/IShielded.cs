using System;

namespace Trans
{
    internal interface IShielded
    {
        bool HasChanges { get; }
        // once having responded to this, implementor must ensure answer stays
        // the same until Commit() or Rollback(). other threads asking to read
        // will get immediate response if their reading stamp is less
        // then this one here. if it is greater, callers spin wait until that
        // Commit() or Rollback() from before happens. calls to CanCommit
        // always spin wait, their stamp is always larger.
        bool CanCommit(bool strict, long writeStamp);
        bool Commit(long writeStamp);
        void Rollback(long? writeStamp = null);
        void TrimCopies(long smallestOpenTransactionId);
    }
}

