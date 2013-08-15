using System;
using System.Threading;
using System.Collections.Generic;
using System.Linq;

namespace Shielded
{
    public class VersionList
    {
        private SortedList<long, int> _items = new SortedList<long, int>();
        private int _busy = 0;

        public long SafeAdd(Func<long> generator)
        {
            if (generator == null)
                throw new ArgumentNullException();

            SpinWait.SpinUntil(() =>
                               Interlocked.CompareExchange(ref _busy, 1, 0) == 0);
            long i;
            try
            {
                i = generator();
                if (_items.ContainsKey(i))
                    _items [i] = _items [i] + 1;
                else
                    _items.Add(i, 1);
            }
            finally
            {
                Interlocked.Exchange(ref _busy, 0);
            }
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
                    _items.Remove(i);
            }
            Interlocked.Exchange(ref _busy, 0);
        }

        public long? Min()
        {
            SpinWait.SpinUntil(() =>
                               Interlocked.CompareExchange(ref _busy, 1, 0) == 0);
            var res = _items.Any() ? _items.First().Key : (long?) null;
            Interlocked.Exchange(ref _busy, 0);
            return res;
        }
    }
}

