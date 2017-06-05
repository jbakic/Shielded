using System;

namespace Shielded
{
    /// <summary>
    /// Exception thrown to signal a rollback is inevitable. Thrown by fields
    /// when they are certain they will not allow the commit, and by <see cref="Shield.Rollback"/>.
    /// Even if you catch this, the transaction will roll back and retry.
    /// </summary>
    public sealed class TransException : Exception
    {
        internal TransException(string message) : base(message)
        {
        }
    }
}

