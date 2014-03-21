using System;
using System.Threading;
using System.Collections.Generic;

namespace Shielded
{
    internal class LocalStorageHold
    {
        [ThreadStatic]
        public static Dictionary<object, object> Storage;
    }

    public class LocalStorage<T> where T : class
    {
        // These two are faster, immediate storage, which can be used by one thread only.
        // If there is more than one, the _storage is used by the others.
        private volatile int _holderThreadId;
        private T _heldValue;

        public bool HasValue
        {
            get
            {
                return _holderThreadId == Thread.CurrentThread.ManagedThreadId ||
                    (LocalStorageHold.Storage != null && LocalStorageHold.Storage.ContainsKey(this));
            }
        }

        public T Value
        {
            get
            {
                return _holderThreadId == Thread.CurrentThread.ManagedThreadId ?
                    _heldValue : (T)LocalStorageHold.Storage[this];
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
                        if (LocalStorageHold.Storage == null)
                            LocalStorageHold.Storage = new Dictionary<object, object>();
                        LocalStorageHold.Storage[this] = value;
                    }
                }
                else if (_holderThreadId == threadId)
                {
                    _heldValue = null;
                    _holderThreadId = 0;
                }
                else if (LocalStorageHold.Storage != null)
                {
                    LocalStorageHold.Storage.Remove(this);
                }
            }
        }
    }
}

