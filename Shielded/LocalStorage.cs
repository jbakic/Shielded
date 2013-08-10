using System;
using System.Threading;
using System.Collections.Concurrent;

namespace Shielded
{
    public class LocalStorage<T> where T : class
    {
        private ConcurrentDictionary<int, T> _storage = new ConcurrentDictionary<int, T>();

        public bool HasValue
        {
            get
            {
                return _storage.ContainsKey(Thread.CurrentThread.ManagedThreadId);
            }
        }

        public T Value
        {
            get
            {
                return _storage[Thread.CurrentThread.ManagedThreadId];
            }
            set
            {
                if (value != null)
                    _storage[Thread.CurrentThread.ManagedThreadId] = value;
                else
                {
                    T x;
                    _storage.TryRemove(Thread.CurrentThread.ManagedThreadId, out x);
                }
            }
        }
    }
}

