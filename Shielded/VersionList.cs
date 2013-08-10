using System;
using System.Threading;
using System.Collections.Generic;
using System.Linq;

namespace Shielded
{
    public class VersionList
    {
        private Dictionary<long, int> _items = new Dictionary<long, int>();
        private long _min = long.MaxValue;
        private int _busy = 0;

        public long SafeAdd(Func<long> generator)
        {
            if (generator == null)
                throw new ArgumentNullException();

            SpinWait.SpinUntil(() =>
                               Interlocked.CompareExchange(ref _busy, 1, 0) == 0
            );
            var i = generator();
            if (_items.ContainsKey(i))
                _items [i] = _items [i] + 1;
            else
            {
                _items.Add(i, 1);
                if (i < _min)
                    _min = i;
            }
            Interlocked.Exchange(ref _busy, 0);
            return i;
        }

        public void Remove(long i)
        {
            SpinWait.SpinUntil(() =>
                               Interlocked.CompareExchange(ref _busy, 1, 0) == 0
            );
            if (_items.ContainsKey(i))
            {
                if (_items[i] > 1)
                    _items[i] = _items[i] - 1;
                else
                {
                    _items.Remove(i);
                    if (_min == i)
                        _min = _items.Any() ? _items.Keys.Min() : long.MaxValue;
                }
            }
            Interlocked.Exchange(ref _busy, 0);
        }

        public long? Min()
        {
            SpinWait.SpinUntil(() =>
                               Interlocked.CompareExchange(ref _busy, 1, 0) == 0);
            var res = _min == long.MaxValue ? (long?)null : _min;
            Interlocked.Exchange(ref _busy, 0);
            return res;
        }
    }
}

