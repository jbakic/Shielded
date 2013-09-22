using System;
using System.Linq;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Shielded
{
    /// <summary>
    /// A dictionary protecting itself only - adding, removing, replacing items are transactional,
    /// but anything done to the items is not unless they are Shielded themselves.
    /// 
    /// The dictionary does not provide any iterators, and considers null as empty,
    /// behaving as an infinite line of slots. Transactional protection for iterating over
    /// a ConcurrentDictionary (this dict is based on it) is difficult, and transactions which
    /// would iterate would be highly conflicting. If needed, use a tree instead, or combine
    /// this with one or more sequences or trees. Iterators might still be added in the future.
    /// </summary>
    public class ShieldedDict<TKey, TItem> : IShielded
    {
        private class ItemKeeper
        {
            public long Version;
            public TItem Value;
            public ItemKeeper Older;
        }

        private class LocalDict
        {
            public Dictionary<TKey, TItem> Items;
            public HashSet<TKey> Reads = new HashSet<TKey>();
        }

        private readonly ConcurrentDictionary<TKey, ItemKeeper> _dict;
        private readonly ConcurrentDictionary<TKey, long> _writeStamps
            = new ConcurrentDictionary<TKey, long>();
        private LocalStorage<LocalDict> _localDict = new LocalStorage<LocalDict>();

        public ShieldedDict(IEnumerable<KeyValuePair<TKey, TItem>> items)
        {
            _dict = new ConcurrentDictionary<TKey, ItemKeeper>(items
                .Select(kvp =>
                    new KeyValuePair<TKey, ItemKeeper>(kvp.Key, new ItemKeeper() { Value = kvp.Value })));
        }

        public ShieldedDict()
        {
            _dict = new ConcurrentDictionary<TKey, ItemKeeper>();
        }

        private void CheckLockAndEnlist(TKey key)
        {
            var stamp = Shield.CurrentTransactionStartStamp;
            SpinWait.SpinUntil(() =>
            {
                long w;
                return !_writeStamps.TryGetValue(key, out w) || w > stamp;
            });
            Shield.Enlist(this);
        }

        private ItemKeeper CurrentTransactionOldValue(TKey key)
        {
            PrepareLocal(key);

            ItemKeeper point;
            _dict.TryGetValue(key, out point);
            while (point != null && point.Version > Shield.CurrentTransactionStartStamp)
                point = point.Older;
            return point;
        }

        private bool IsLocalPrepared()
        {
            return _localDict.HasValue;
        }

        private void PrepareLocal(TKey key)
        {
            CheckLockAndEnlist(key);
            if (!_localDict.HasValue)
                _localDict.Value = new LocalDict();
            _localDict.Value.Reads.Add(key);
        }

        /// <summary>
        /// Never throws, just returns null if it does not have anything.
        /// </summary>
        public TItem this [TKey key]
        {
            get
            {
                ItemKeeper v;
                if (!Shield.IsInTransaction || !IsLocalPrepared() ||
                    _localDict.Value.Items == null || !_localDict.Value.Items.ContainsKey(key))
                {
                    if (Shield.IsInTransaction)
                        v = CurrentTransactionOldValue(key);
                    else if (!_dict.TryGetValue(key, out v))
                        return default(TItem);
                    return v == null ? default(TItem) : v.Value;
                }
                else if (_dict.TryGetValue(key, out v) && v.Version > Shield.CurrentTransactionStartStamp)
                    throw new TransException("Writable read collision.");
                return _localDict.Value.Items[key];
            }
            set
            {
                PrepareLocal(key);
                if (_localDict.Value.Items == null)
                    _localDict.Value.Items = new Dictionary<TKey, TItem>();
                _localDict.Value.Items[key] = value;
            }
        }

        bool IShielded.HasChanges
        {
            get
            {
                return IsLocalPrepared() && _localDict.Value.Items != null;
            }
        }

        bool IShielded.CanCommit(long writeStamp)
        {
            // locals were prepared when we enlisted.
            if (_localDict.Value.Reads.Any(key =>
                    {
                        ItemKeeper v;
                        return _writeStamps.ContainsKey(key) ||
                            (_dict.TryGetValue(key, out v) && v.Version > Shield.CurrentTransactionStartStamp);
                    }))
                return false;
            else
            {
                // touch only the ones we plan to change
                if (_localDict.Value.Items != null)
                    foreach (var key in _localDict.Value.Items.Keys)
                    {
                        if (!_writeStamps.TryAdd(key, writeStamp))
                            throw new ApplicationException("Another transaction already has write lock on this key!");
                    }
                return true;
            }
        }

        private ConcurrentDictionary<TKey, long> _copies = new ConcurrentDictionary<TKey, long>();

        bool IShielded.Commit(long? writeStamp)
        {
            if (((IShielded)this).HasChanges)
            {
                foreach (var kvp in _localDict.Value.Items)
                {
                    ItemKeeper v = null;
                    _dict.TryGetValue(kvp.Key, out v);
                    var newCurrent = new ItemKeeper()
                    {
                        Value = kvp.Value,
                        Version = writeStamp.Value,
                        Older = v
                    };
                    lock (_dict)
                        _dict[kvp.Key] = newCurrent;
                    _copies[kvp.Key] = writeStamp.Value;

                    long ourStamp;
                    if (!_writeStamps.TryRemove(kvp.Key, out ourStamp) || ourStamp != writeStamp)
                        throw new ApplicationException("Commit from unexpected transaction");
                }
                _localDict.Value = null;
                return true;
            }
            _localDict.Value = null;
            return false;
        }

        void IShielded.Rollback(long? writeStamp)
        {
            if (_localDict.Value.Items != null && writeStamp.HasValue)
            {
                long ws;
                foreach (var key in _localDict.Value.Items.Keys)
                    if (_writeStamps.TryGetValue(key, out ws) && ws == writeStamp.Value)
                        _writeStamps.TryRemove(key, out ws);
            }
            _localDict.Value = null;
        }

        void IShielded.TrimCopies(long smallestOpenTransactionId)
        {
            // NB the "smallest transaction" and others can freely read while
            // we're doing this.
            var keys = _copies.Keys;
            foreach (var key in keys)
            {
                if (_copies[key] > smallestOpenTransactionId)
                    continue;

                if (_dict.ContainsKey(key))
                {
                    var point = _dict[key];
                    ItemKeeper pointNewer = null;
                    while (point != null && point.Version > smallestOpenTransactionId)
                    {
                        pointNewer = point;
                        point = point.Older;
                    }
                    if (point != null)
                    {
                        // point is the last accessible - his Older is not needed.
                        point.Older = null;
                        if (point.Value == null)
                        {
                            if (pointNewer != null)
                                pointNewer.Older = null;
                            else
                            {
                                //((ICollection<KeyValuePair<TKey, ItemKeeper>>)_dict)
                                //    .Remove(new KeyValuePair<TKey, ItemKeeper>(key, point));
                                lock (_dict)
                                {
                                    ItemKeeper k;
                                    if (_dict.TryGetValue(key, out k) && k == point)
                                        _dict.TryRemove(key, out k);
                                }
                            }
                        }
                    }
                }
                long version;
                _copies.TryRemove(key, out version);
                if (version > smallestOpenTransactionId)
                    _copies.TryAdd(key, version);
            }
        }
    }
}
