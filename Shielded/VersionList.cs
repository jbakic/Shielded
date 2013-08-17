using System;
using System.Threading;
using System.Collections.Generic;
using System.Linq;

namespace Shielded
{
    public class VersionList
    {
        private Dictionary<long, int> _items = new Dictionary<long, int>();
        private long? _min;
        private int _busy = 0;

        public long SafeAdd(Func<long> generator)
        {
            if (generator == null)
                throw new ArgumentNullException();

            long i;
            bool gotBusy = false;
            try
            {
                SpinWait.SpinUntil(() =>
                    gotBusy = Interlocked.CompareExchange(ref _busy, 1, 0) == 0);
                i = generator();
                if (_items.ContainsKey(i))
                    _items [i] = _items [i] + 1;
                else
                {
                    _items.Add(i, 1);
                    if (_min == null || _min > i) _min = i;
                }
            }
            finally
            {
                if (gotBusy) Interlocked.Exchange(ref _busy, 0);
            }
            return i;
        }

        public void Remove(long i)
        {
            bool gotBusy = false;
            try
            {
                SpinWait.SpinUntil(() =>
                    gotBusy = Interlocked.CompareExchange(ref _busy, 1, 0) == 0);
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
            finally
            {
                if (gotBusy) Interlocked.Exchange(ref _busy, 0);
            }
        }

        public long? Min()
        {
            bool gotBusy = false;
            try
            {
                SpinWait.SpinUntil(() =>
                    gotBusy = Interlocked.CompareExchange(ref _busy, 1, 0) == 0);
                //return _items.Any() ? _items.First().Key : (long?)null;
                return _min;
            }
            finally
            {
                if (gotBusy) Interlocked.Exchange(ref _busy, 0);
            }
        }
    }
}

