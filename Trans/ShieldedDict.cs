using System;
using System.Linq;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Trans
{
    public class ShieldedDict<TKey, TItem> where TItem : struct// : IShielded
    {
        private readonly ConcurrentDictionary<TKey, Shielded<TItem>> _dict;

        public ShieldedDict(IEnumerable<TItem> items, Func<TItem, TKey> keySelector)
        {
            // monodevelop doesn't get it :D
            _dict = new ConcurrentDictionary<TKey, Shielded<TItem>>(
                items.Select(i => new KeyValuePair<TKey, Shielded<TItem>>(
                    keySelector(i), new Shielded<TItem>(i))));
        }

        public ShieldedDict()
        {
            _dict = new ConcurrentDictionary<TKey, Shielded<TItem>>();
        }

        public TItem this [TKey key]
        {
            get
            {
                return _dict[key].Read;
            }
        }

        public TItem Read(TKey key)
        {
            return _dict[key].Read;
        }
    }
}

