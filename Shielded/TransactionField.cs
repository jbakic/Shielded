using System;

namespace Shielded
{
    /// <summary>
    /// A descriptor for some object taking part in a transaction.
    /// </summary>
    public struct TransactionField
    {
        /// <summary>
        /// The object which enlisted in the transaction.
        /// </summary>
        public readonly object Field;

        /// <summary>
        /// If the given object has any changes.
        /// </summary>
        public readonly bool HasChanges;

        /// <summary>
        /// Constructor.
        /// </summary>
        public TransactionField(object field, bool hasChanges)
        {
            Field = field;
            HasChanges = hasChanges;
        }
    }
}

