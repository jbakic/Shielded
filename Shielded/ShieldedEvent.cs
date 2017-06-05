using System;
using System.Threading;

namespace Shielded
{
    /// <summary>
    /// Similar to an ordinary .NET event, except that subscribing and unsubscribing
    /// is transactional. This means, for example, that you will not get events until
    /// your transaction, which makes the subscription, has committed. After that, you
    /// are guaranteed to get all of them! (Any transaction which should have raised it
    /// will be in conflict with your transaction, will be retried, and then will raise
    /// the event!) The event is raised directly - all handlers run on the same thread
    /// and inside the transaction that triggers them.
    /// </summary>
    public class ShieldedEvent<T> where T : EventArgs
    {
        private ShieldedSeq<EventHandler<T>> _handlers;

        /// <summary>
        /// Subscribe a new event handler.
        /// </summary>
        public void Subscribe(EventHandler<T> handler)
        {
            if (_handlers == null)
                Interlocked.CompareExchange(ref _handlers, new ShieldedSeq<EventHandler<T>>(), null);
            _handlers.Append(handler);
        }

        /// <summary>
        /// Unsubscribe the specified handler.
        /// </summary>
        public void Unsubscribe(EventHandler<T> handler)
        {
            if (_handlers != null)
                _handlers.Remove(handler);
            else
                throw new InvalidOperationException("Not subscribed.");
        }

        /// <summary>
        /// Raise the event with the given arguments.
        /// </summary>
        public void Raise(object sender, T args)
        {
            if (_handlers == null) return;
            foreach (var h in _handlers)
                h(sender, args);
        }
    }
}

