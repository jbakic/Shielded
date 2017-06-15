using System;

namespace Shielded
{
    /// <summary>
    /// Thrown by <see cref="CommitContinuation"/> methods which depend on the continuation
    /// not being completed. E.g. running <see cref="CommitContinuation.InContext"/> after
    /// the context has completed will throw this exception.
    /// </summary>
    public class ContinuationCompletedException : Exception
    {
    }
}
