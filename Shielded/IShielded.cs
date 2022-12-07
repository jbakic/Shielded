using System;

namespace Shielded
{

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class ShieldedAttribute : Attribute { };

    public interface ICommutable
    {
        // by convention, overriden to execute the action as a commutable, non-conflicting operation
        void Commute(Action a);
    }

    public interface IChangedNotify
    {
        // ...and this one gets called on every change. also called from commutes, in which case trying
        // to access any other shielded field will throw.
        void OnChanged(string property);
    }

    /// <summary>
    /// The <see cref="Shield"/> class tracks implementors of this interface.
    /// </summary>
    internal interface IShielded
    {
        bool HasChanges { get; }

        /// <summary>
        /// The logical owner of the shielded field. Used in Committing events, since external
        /// users cannot access internal fields.
        /// </summary>
        object Owner { get; }

        /// <summary>
        /// Returns true if there have been no changes by other threads since this
        /// transaction opened. If so, and if the field has changes in this transaction,
        /// the method will also lock the field until a call to Commit or Rollback is made.
        /// It is called under the pre-commit lock, so it is safe, but must be quick.
        /// </summary>
        bool CanCommit(WriteStamp writeStamp);

        void Commit();
        void Rollback();

        void TrimCopies(long smallestOpenTransactionId);
    }
}

