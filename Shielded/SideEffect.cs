using System;
using System.Collections.Generic;

namespace Shielded
{
    /// <summary>
    /// Holds a side-effect definition. They are held in one list, each element containing
    /// a possible OnCommit and OnRollback lambda. They will execute in the order they were
    /// defined.
    /// </summary>
    internal class SideEffect
    {
        public readonly Action OnCommit, OnRollback;

        public SideEffect(Action fx, Action rollbackFx)
        {
            OnCommit = fx;
            OnRollback = rollbackFx;
        }
    }
}

