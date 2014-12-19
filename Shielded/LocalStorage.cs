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

    /// <summary>
    /// Thread-local storage - multiple threads can share a ref to this, each sees only
    /// what it put inside. It is very simple, using direct local fields when one thread
    /// is using it, and resorting to a ThreadStatic dictionary otherwise.
    /// </summary>
    public class LocalStorage<T> where T : class
    {
        // These two are faster, immediate storage, which can be used by one thread only.
        // If there is more than one, the _storage is used by the others.
        private int _holderThreadId;
        private T _heldValue;

        /// <summary>
        /// Returns <c>true</c> if current thread has something (non-null!) inside this storage.
        /// </summary>
        public bool HasValue
        {
            get
            {
                return _holderThreadId == Thread.CurrentThread.ManagedThreadId ||
                    (LocalStorageHold.Storage != null && LocalStorageHold.Storage.ContainsKey(this));
            }
        }

        /// <summary>
        /// Gets or sets the value for the current thread. Setting it to null releases
        /// the storage used by the current thread. Getter throws if there is no value
        /// for this thread.
        /// </summary>
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
                    {
                        _heldValue = value;
                        // A bugfix for a bug that never was. -- If we're just now taking over
                        // local fields, then what if we already had something in the dictionary?
                        // Let's remove it then. The bug, however, never happened, because no user
                        // of this class makes two consecutive writes. They put something in,
                        // then null, and only then something else. But it's best to be safe.
                        if (holder == 0 && LocalStorageHold.Storage != null)
                            LocalStorageHold.Storage.Remove(this);
                    }
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

