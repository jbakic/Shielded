using System;

namespace Shielded
{
    internal enum CommuteState
    {
        Ok = 0,

        /// <summary>
        /// Commute must execute due to transaction accessing a field it works on.
        /// </summary>
        Broken,

        /// <summary>
        /// Commute was executed.
        /// </summary>
        Executed
    }

    internal class Commute
    {
        /// <summary>
        /// The commutable action.
        /// </summary>
        public Action Perform;

        /// <summary>
        /// The fields which, if enlisted by the main transaction, must cause the commute to
        /// execute in-transaction.
        /// </summary>
        public ICommutableShielded[] Affecting;

        /// <summary>
        /// The state of the commute.
        /// </summary>
        public CommuteState State;
    }
}

