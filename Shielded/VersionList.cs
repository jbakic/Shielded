using System;
using System.Threading;
using System.Collections.Generic;
using System.Linq;

namespace Shielded
{
    internal class VersionList
    {
        private Dictionary<long, int> _items = new Dictionary<long, int>();
        private long? _min;
        private object _lock = new object();

        public long SafeAdd(Func<long> generator)
        {
            if (generator == null)
                throw new ArgumentNullException();

            lock (_lock)
            {
                var i = generator();
                if (_items.ContainsKey(i))
                    _items [i] = _items [i] + 1;
                else
                {
                    _items.Add(i, 1);
                    if (_min == null || _min > i) _min = i;
                }
                return i;
            }
        }

        public void Remove(long i)
        {
            lock (_lock)
            {
                if (_items.ContainsKey(i))
                {
                    if (_items [i] > 1)
                        _items [i] = _items [i] - 1;
                    else
                    {
                        _items.Remove(i);
                        if (_min == i)
                            _min = _items.Any() ? _items.Keys.Min() : (long?)null;
                    }
                }
            }
        }

        public long? Min()
        {
            lock (_lock)
            {
                //return _items.Any() ? _items.First().Key : (long?)null;
                return _min;
            }
        }
    }
}

