using System;
using System.Threading;
using System.Collections.Generic;

namespace Shielded
{
    public class LocalStorage<T> where T : class
    {
        [ThreadStatic]
        private static Dictionary<LocalStorage<T>, T> _storage;

        public bool HasValue
        {
            get
            {
                return _storage != null && _storage.ContainsKey(this);
            }
        }

        public T Value
        {
            get
            {
                return _storage[this];
            }
            set
            {
                if (value != null)
                {
                    if (_storage == null)
                        _storage = new Dictionary<LocalStorage<T>, T>();
                    _storage[this] = value;
                }
                else if (_storage != null)
                {
                    _storage.Remove(this);
                }
            }
        }
    }
}

