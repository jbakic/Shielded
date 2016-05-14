using System;
using System.Collections.Generic;

namespace Shielded
{
    internal abstract class TransactionContext : CommitContinuation
    {
        public Dictionary<object, object> Storage;
    }
}

