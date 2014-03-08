using System;
using System.Threading;
using System.Collections.Generic;

namespace Shielded
{
    public class LocalStorage<T> where T : class
    {
        [ThreadStatic]
        private static Dictionary<LocalStorage<T>, T> _storage;

        // These two are faster, immediate storage, which can be used by one thread only.
        // If there is more than one, the _storage is used by the others.
        private volatile int _holderThreadId;
        private T _heldValue;

        public bool HasValue
        {
            get
            {
                return _holderThreadId == Thread.CurrentThread.ManagedThreadId ||
                    (_storage != null && _storage.ContainsKey(this));
            }
        }

        public T Value
        {
            get
            {
                return _holderThreadId == Thread.CurrentThread.ManagedThreadId ? _heldValue : _storage[this];
            }
            set
            {
                var threadId = Thread.CurrentThread.ManagedThreadId;
                if (value != null)
                {
                    var holder = Interlocked.CompareExchange(ref _holderThreadId, threadId, 0);
                    if (holder == threadId || holder == 0)
                        _heldValue = value;
                    else
                    {
                        if (_storage == null)
                            _storage = new Dictionary<LocalStorage<T>, T>();
                        _storage[this] = value;
                    }
                }
                else if (_holderThreadId == threadId)
                {
                    _heldValue = null;
                    _holderThreadId = 0;
                }
                else if (_storage != null)
                {
                    _storage.Remove(this);
                }
            }
        }
    }
}

