using System;
using System.Threading;

namespace Shielded
{
    public class ShieldedEvent<T> where T : EventArgs
    {
        private ShieldedSeq<EventHandler<T>> _handlers;

        public void Subscribe(EventHandler<T> handler)
        {
            if (_handlers == null)
                Interlocked.CompareExchange(ref _handlers, new ShieldedSeq<EventHandler<T>>(), null);
            _handlers.Append(handler);
        }

        public void Unsubscribe(EventHandler<T> handler)
        {
            _handlers.Remove(handler);
        }

        public void Raise(object sender, T args)
        {
            if (_handlers == null) return;
            foreach (var h in _handlers)
                h(sender, args);
        }
    }
}

