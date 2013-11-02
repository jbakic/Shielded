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
    /// Iterating over this is highly conflicting - any other transaction that changes the structure
    /// of the dictionary and commits, will cause you to be rolled back (unless, of course, you're
    /// a read-only transaction).
    /// </summary>
    public class ShieldedDict<TKey, TItem> : IShielded, IDictionary<TKey, TItem>
    {
        private class ItemKeeper
        {
            public long Version;
            public TItem Value;
            public bool Empty;
            public ItemKeeper Older;
        }

        private class LocalDict
        {
            public Dictionary<TKey, ItemKeeper> Items;
            public HashSet<TKey> Reads = new HashSet<TKey>();
        }

        private readonly ConcurrentDictionary<TKey, ItemKeeper> _dict;
        // This is the only protection for iterators - each iterating op will read the count, just
        // to provoke a conflict with any transaction that modifies the Count! Simple as hell.
        private readonly Shielded<int> _count;
        private readonly ConcurrentDictionary<TKey, Tuple<int, long>> _writeStamps
            = new ConcurrentDictionary<TKey, Tuple<int, long>>();
        private readonly LocalStorage<LocalDict> _localDict = new LocalStorage<LocalDict>();

        public ShieldedDict(IEnumerable<KeyValuePair<TKey, TItem>> items)
        {
            _dict = new ConcurrentDictionary<TKey, ItemKeeper>(items
                .Select(kvp =>
                    new KeyValuePair<TKey, ItemKeeper>(kvp.Key, new ItemKeeper() { Value = kvp.Value })));
            _count = new Shielded<int>(_dict.Count);
        }

        public ShieldedDict()
        {
            _dict = new ConcurrentDictionary<TKey, ItemKeeper>();
            _count = new Shielded<int>(0);
        }

        private void CheckLockAndEnlist(TKey key)
        {
            var stamp = Shield.CurrentTransactionStartStamp;
#if SERVER
            Tuple<int, long> w;
            if (_writeStamps.TryGetValue(key, out w) && w.Item2 <= stamp)
                lock (_localDict)
                {
                    while (_writeStamps.TryGetValue(key, out w) && w.Item2 <= stamp)
                        Monitor.Wait(_localDict);
                }
#else
            SpinWait.SpinUntil(() =>
            {
                Tuple<int, long> w;
                return !_writeStamps.TryGetValue(key, out w) || w.Item2 > stamp;
            });
#endif
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
                        throw new KeyNotFoundException();
                    if (v == null || v.Empty)
                        throw new KeyNotFoundException();
                    return v.Value;
                }
                else if (_dict.TryGetValue(key, out v) && v.Version > Shield.CurrentTransactionStartStamp)
                    throw new TransException("Writable read collision.");

                if (_localDict.Value.Items[key].Empty)
                    throw new KeyNotFoundException();
                return _localDict.Value.Items[key].Value;
            }
            set
            {
                PrepareLocal(key);
                ItemKeeper curr;
                bool hasValue;
                if ((hasValue = _dict.TryGetValue(key, out curr)) && curr.Version > Shield.CurrentTransactionStartStamp)
                    throw new TransException("Write collision.");

                if (_localDict.Value.Items == null)
                    _localDict.Value.Items = new Dictionary<TKey, ItemKeeper>();
                else if (_localDict.Value.Items.ContainsKey(key))
                {
                    if (_localDict.Value.Items[key].Empty)
                    {
                        _count.Commute((ref int n) => n++);
                        _localDict.Value.Items[key].Empty = false;
                    }
                    _localDict.Value.Items[key].Value = value;
                    return;
                }
                _localDict.Value.Items[key] = new ItemKeeper() { Value = value };
                if (!hasValue || curr.Empty)
                    _count.Commute((ref int n) => n++);
            }
        }

        bool IShielded.HasChanges
        {
            get
            {
                return IsLocalPrepared() && _localDict.Value.Items != null;
            }
        }

        bool IShielded.CanCommit(Tuple<int, long> writeStamp)
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
                        _writeStamps[key] = writeStamp;
                return true;
            }
        }

        private ConcurrentDictionary<TKey, long> _copies = new ConcurrentDictionary<TKey, long>();

        void IShielded.Commit()
        {
            if (_localDict.Value.Items != null)
            {
                foreach (var kvp in _localDict.Value.Items)
                {
                    var ws = _writeStamps[kvp.Key];
                    ItemKeeper v = null;
                    _dict.TryGetValue(kvp.Key, out v);

                    var newCurrent = kvp.Value;
                    newCurrent.Version = ws.Item2;
                    newCurrent.Older = v;
                    lock (_dict)
                        _dict[kvp.Key] = newCurrent;
                    _copies[kvp.Key] = ws.Item2;

#if SERVER
                    lock (_localDict)
                    {
                        _writeStamps.TryRemove(kvp.Key, out ws);
                        Monitor.PulseAll(_localDict);
                    }
#else
                    _writeStamps.TryRemove(kvp.Key, out ws);
#endif
                }
            }
            _localDict.Value = null;
        }

        void IShielded.Rollback()
        {
            if (!_localDict.HasValue)
                return;
            if (_localDict.Value.Items != null)
            {
                Tuple<int, long> ws;
#if SERVER
                lock (_localDict)
                {
                    foreach (var key in _localDict.Value.Items.Keys)
                        if (_writeStamps.TryGetValue(key, out ws) && ws.Item1 == Thread.CurrentThread.ManagedThreadId)
                            _writeStamps.TryRemove(key, out ws);
                    Monitor.PulseAll(_localDict);
                }
#else
                foreach (var key in _localDict.Value.Items.Keys)
                    if (_writeStamps.TryGetValue(key, out ws) && ws.Item1 == Thread.CurrentThread.ManagedThreadId)
                        _writeStamps.TryRemove(key, out ws);
#endif
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
                        if (point.Empty)
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

        #region IEnumerable implementation
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<KeyValuePair<TKey, TItem>>)this).GetEnumerator();
        }
        #endregion

        #region IEnumerable implementation
        public IEnumerator<KeyValuePair<TKey, TItem>> GetEnumerator()
        {
            Shield.AssertInTransaction();
            // force conflict if _count changes. even if it changes back to same value!
            var c = _count.Read;
            var keys = _localDict.HasValue && _localDict.Value.Items != null ?
                _dict.Keys.Except(_localDict.Value.Items.Keys) : _dict.Keys;
            foreach (var key in keys)
            {
                var v = CurrentTransactionOldValue(key);
                if (v == null || v.Empty) continue;
                yield return new KeyValuePair<TKey, TItem>(key, v.Value);
            }
            if (_localDict.HasValue && _localDict.Value.Items != null)
                foreach (var key in _localDict.Value.Items.Keys)
                {
                    var v = _localDict.Value.Items[key];
                    if (v.Empty) continue;
                    yield return new KeyValuePair<TKey, TItem>(key, v.Value);
                }
        }
        #endregion

        #region ICollection implementation
        public void Add(KeyValuePair<TKey, TItem> item)
        {
            Add(item.Key, item.Value);
        }

        /// <summary>
        /// Removes them one by one.
        /// </summary>
        public void Clear()
        {
            foreach (var key in Keys)
                Remove(key);
        }

        /// <summary>
        /// Checks the key only.
        /// </summary>
        public bool Contains(KeyValuePair<TKey, TItem> item)
        {
            return ContainsKey(item.Key);
        }

        public void CopyTo(KeyValuePair<TKey, TItem>[] array, int arrayIndex)
        {
            Shield.InTransaction(() => {
                if (_count + arrayIndex > array.Length)
                    throw new IndexOutOfRangeException();
                foreach (KeyValuePair<TKey, TItem> kvp in this)
                    array[arrayIndex++] = kvp;
            });
        }

        /// <summary>
        /// Checks the key only, removed value may be different.
        /// </summary>
        public bool Remove(KeyValuePair<TKey, TItem> item)
        {
            return Remove(item.Key);
        }

        public int Count
        {
            get
            {
                return _count;
            }
        }

        public bool IsReadOnly
        {
            get
            {
                return false;
            }
        }
        #endregion

        #region IDictionary implementation
        public void Add(TKey key, TItem value)
        {
            Shield.AssertInTransaction();
            if (ContainsKey(key))
                throw new ArgumentException("The given key is already present in the dictionary.");
            this[key] = value;
        }

        public bool ContainsKey(TKey key)
        {
            ItemKeeper v;
            if (!Shield.IsInTransaction)
                return _dict.TryGetValue(key, out v) && !v.Empty;

            v = _localDict.HasValue && _localDict.Value.Items != null &&
                _localDict.Value.Items.ContainsKey(key) ?
                    _localDict.Value.Items[key] : CurrentTransactionOldValue(key);
            return v != null && !v.Empty;
        }

        public bool Remove(TKey key)
        {
            Shield.AssertInTransaction();
            if (!ContainsKey(key))
                return false;

            if (_localDict.Value.Items == null)
                _localDict.Value.Items = new Dictionary<TKey, ItemKeeper>();
            _localDict.Value.Items[key] = new ItemKeeper() { Empty = true };

            _count.Commute((ref int n) => n--);
            return true;
        }

        public bool TryGetValue(TKey key, out TItem value)
        {
            if (!Shield.IsInTransaction)
            {
                ItemKeeper v;
                if (_dict.TryGetValue(key, out v) && !v.Empty)
                {
                    value = v.Value;
                    return true;
                }
                value = default(TItem);
                return false;
            }

            var c = _localDict.HasValue && _localDict.Value.Items != null &&
                _localDict.Value.Items.ContainsKey(key) ?
                    _localDict.Value.Items [key] : CurrentTransactionOldValue(key);
            if (c != null && !c.Empty)
            {
                value = c.Value;
                return true;
            }
            value = default(TItem);
            return false;
        }

        public ICollection<TKey> Keys
        {
            get
            {
                return ((IEnumerable<KeyValuePair<TKey, TItem>>)this)
                    .Select(kvp => kvp.Key)
                    .ToList();
            }
        }

        public ICollection<TItem> Values
        {
            get
            {
                return ((IEnumerable<KeyValuePair<TKey, TItem>>)this)
                    .Select(kvp => kvp.Value)
                    .ToList();
            }
        }

        #endregion

    }
}
