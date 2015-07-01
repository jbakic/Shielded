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
            public Dictionary<TKey, ItemKeeper> Items = new Dictionary<TKey, ItemKeeper>();
            public bool HasChanges;
            public bool Locked;
        }

        private readonly ConcurrentDictionary<TKey, ItemKeeper> _dict;
        // This is the only protection for iterators - each iterating op will read the count, just
        // to provoke a conflict with any transaction that modifies the Count! Simple as hell.
        private readonly Shielded<int> _count;
        private readonly ConcurrentDictionary<TKey, WriteStamp> _writeStamps =
            new ConcurrentDictionary<TKey, WriteStamp>();
        private readonly LocalStorage<LocalDict> _localDict = new LocalStorage<LocalDict>();
        private readonly StampLocker _locker = new StampLocker();

        /// <summary>
        /// Initializes a new instance with the given initial contents.
        /// </summary>
        public ShieldedDict(IEnumerable<KeyValuePair<TKey, TItem>> items)
        {
            _dict = new ConcurrentDictionary<TKey, ItemKeeper>(items
                .Select(kvp =>
                    new KeyValuePair<TKey, ItemKeeper>(kvp.Key, new ItemKeeper() { Value = kvp.Value })));
            _count = new Shielded<int>(_dict.Count);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public ShieldedDict()
        {
            _dict = new ConcurrentDictionary<TKey, ItemKeeper>();
            _count = new Shielded<int>(0);
        }

        bool LockCheck(TKey key)
        {
            WriteStamp w;
            return !_writeStamps.TryGetValue(key, out w) || w.Version == null || w.Version > Shield.ReadStamp;
        }

        private void CheckLockAndEnlist(TKey key, bool write)
        {
            var locals = _localDict.HasValue ? _localDict.Value : null;
            if (locals != null && locals.Locked)
            {
                CheckLockedAccess(key, write);
                return; // because the check above is much stricter than anything
            }
            if (!Shield.Enlist(this, locals != null, write) && locals.Items.ContainsKey(key))
                return;

            if (!LockCheck(key))
                _locker.WaitUntil(() => LockCheck(key));
        }

        /// <summary>
        /// Since the dictionary is just one field to the Shield class, we internally check, if
        /// the access is happening while locked, whether the access is safe.
        /// </summary>
        void CheckLockedAccess(TKey key, bool write)
        {
            var locals = _localDict.Value;
            ItemKeeper item;
            if (!locals.Items.TryGetValue(key, out item))
                throw new InvalidOperationException("No new key access in this context.");
            if (item == null && write)
                throw new InvalidOperationException("No new writes in this context.");
        }

        private ItemKeeper CurrentTransactionOldValue(TKey key)
        {
            ItemKeeper point;
            _dict.TryGetValue(key, out point);
            while (point != null && point.Version > Shield.ReadStamp)
                point = point.Older;
            return point;
        }

        private LocalDict PrepareLocals()
        {
            LocalDict locals;
            if (_localDict.HasValue)
                locals = _localDict.Value;
            else
                _localDict.Value = locals = new LocalDict();
            return locals;
        }

        /// <summary>
        /// Returns an existing local entry, if any.
        /// </summary>
        private ItemKeeper PrepareWrite(TKey key)
        {
            CheckLockAndEnlist(key, true);
            var locals = PrepareLocals();
            ItemKeeper existing;
            if (locals.Items.TryGetValue(key, out existing))
                return existing;
            return null;
        }

        private ItemKeeper Read(TKey key)
        {
            CheckLockAndEnlist(key, false);
            ItemKeeper v, curr;
            var locals = PrepareLocals();
            bool hasLocal;
            if (!(hasLocal = locals.Items.TryGetValue(key, out v)) || v == null)
            {
                if (!hasLocal)
                    locals.Items.Add(key, null);
                v = CurrentTransactionOldValue(key);
            }
            else if (_dict.TryGetValue(key, out curr) && curr.Version > Shield.ReadStamp)
                throw new TransException("Writable read collision.");
            return v;
        }

        /// <summary>
        /// Gets or sets the value under the specified key.
        /// </summary>
        public TItem this [TKey key]
        {
            get
            {
                ItemKeeper v;
                if (!Shield.IsInTransaction)
                {
                    if (!_dict.TryGetValue(key, out v) || v.Empty)
                        throw new KeyNotFoundException();
                    return v.Value;
                }

                v = Read(key);
                if (v == null || v.Empty)
                    throw new KeyNotFoundException();
                return v.Value;
            }
            set
            {
                ItemKeeper local = PrepareWrite(key);
                ItemKeeper curr;
                bool hasValue;
                if ((hasValue = _dict.TryGetValue(key, out curr)) && curr.Version > Shield.ReadStamp)
                    throw new TransException("Write collision.");

                if (local == null)
                {
                    var locals = _localDict.Value;
                    locals.Items[key] = new ItemKeeper() { Value = value };
                    locals.HasChanges = true;
                    if (!hasValue || curr.Empty)
                        _count.Commute((ref int n) => n++);
                }
                else
                {
                    if (local.Empty)
                    {
                        _count.Commute((ref int n) => n++);
                        local.Empty = false;
                    }
                    local.Value = value;
                }
            }
        }

        /// <summary>
        /// An enumerable of keys for which the current transaction made changes in the dictionary.
        /// Safely accessible from <see cref="Shield.WhenCommitting"/> subscriptions. NB that
        /// this also includes keys which were removed from the dictionary.
        /// </summary>
        public IEnumerable<TKey> Changes
        {
            get
            {
                if (Shield.IsInTransaction && _localDict.HasValue && _localDict.Value.HasChanges)
                {
                    return _localDict.Value.Items
                        .Where(kvp => kvp.Value != null)
                        .Select(kvp => kvp.Key);
                }
                return Enumerable.Empty<TKey>();
            }
        }

        bool IShielded.HasChanges
        {
            get
            {
                return _localDict.HasValue && _localDict.Value.HasChanges;
            }
        }

        object IShielded.Owner
        {
            get
            {
                return this;
            }
        }

        bool IShielded.CanCommit(WriteStamp writeStamp)
        {
            var locals = _localDict.Value;
            locals.Locked = true;
            // locals were prepared when we enlisted.
            if (locals.Items.Any(kvp =>
                    {
                        ItemKeeper v;
                        return _writeStamps.ContainsKey(kvp.Key) ||
                            (_dict.TryGetValue(kvp.Key, out v) && v.Version > Shield.ReadStamp);
                    }))
                return false;
            else
            {
                // touch only the ones we plan to change
                if (locals.HasChanges)
                    foreach (var kvp in locals.Items)
                        if (kvp.Value != null)
                            _writeStamps[kvp.Key] = writeStamp;
                return true;
            }
        }

        private ConcurrentQueue<Tuple<long, List<TKey>>> _copies =
            new ConcurrentQueue<Tuple<long, List<TKey>>>();

        void IShielded.Commit()
        {
            var locals = _localDict.Value;
            if (locals.HasChanges)
            {
                long? version = null;
                var copyList = new List<TKey>();
                foreach (var kvp in _localDict.Value.Items)
                {
                    if (kvp.Value == null)
                        continue;
                    if (version == null)
                        version = _writeStamps[kvp.Key].Version;

                    ItemKeeper v = null;
                    if (_dict.TryGetValue(kvp.Key, out v))
                        copyList.Add(kvp.Key);

                    var newCurrent = kvp.Value;
                    newCurrent.Version = (long)version;
                    newCurrent.Older = v;
                    lock (_dict)
                        _dict[kvp.Key] = newCurrent;

                    WriteStamp ws;
                    _writeStamps.TryRemove(kvp.Key, out ws);
                }
                if (version.HasValue)
                    _copies.Enqueue(Tuple.Create((long)version, copyList));
                _locker.Release();
            }
            _localDict.Release();
        }

        void IShielded.Rollback()
        {
            if (!_localDict.HasValue)
                return;
            var locals = _localDict.Value;
            if (locals.HasChanges)
            {
                WriteStamp ws;
                foreach (var kvp in locals.Items)
                {
                    if (kvp.Value != null &&
                        _writeStamps.TryGetValue(kvp.Key, out ws) &&
                        ws.ThreadId == Thread.CurrentThread.ManagedThreadId)
                    {
                        _writeStamps.TryRemove(kvp.Key, out ws);
                    }
                }
                _locker.Release();
            }
            _localDict.Release();
        }

        void IShielded.TrimCopies(long smallestOpenTransactionId)
        {
            // NB the "smallest transaction" and others can freely read while
            // we're doing this.
            Tuple<long, List<TKey>> item;
            while (_copies.TryPeek(out item) && item.Item1 < smallestOpenTransactionId)
            {
                _copies.TryDequeue(out item);
                foreach (var key in item.Item2)
                {
                    ItemKeeper point;
                    if (!_dict.TryGetValue(key, out point))
                        continue;
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
            }
        }

        #region IEnumerable implementation
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<KeyValuePair<TKey, TItem>>)this).GetEnumerator();
        }
        #endregion

        #region IEnumerable implementation
        /// <summary>
        /// Get an enumerator for the dictionary contents. Iterating over a dictionary
        /// conflicts with any concurrent write on the dictionary (of course, not if
        /// your transaction is read-only).
        /// </summary>
        public IEnumerator<KeyValuePair<TKey, TItem>> GetEnumerator()
        {
            Shield.AssertInTransaction();
            // force conflict if _count changes. even if it changes back to same value!
            var c = _count.Value;
            var keys = _localDict.HasValue ?
                _dict.Keys.Union(_localDict.Value.Items.Keys) : _dict.Keys;
            foreach (var key in keys)
            {
                var v = Read(key);
                if (v == null || v.Empty) continue;
                yield return new KeyValuePair<TKey, TItem>(key, v.Value);
            }
        }
        #endregion

        #region ICollection implementation
        void ICollection<KeyValuePair<TKey, TItem>>.Add(KeyValuePair<TKey, TItem> item)
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
        bool ICollection<KeyValuePair<TKey, TItem>>.Contains(KeyValuePair<TKey, TItem> item)
        {
            return ContainsKey(item.Key);
        }

        void ICollection<KeyValuePair<TKey, TItem>>.CopyTo(KeyValuePair<TKey, TItem>[] array, int arrayIndex)
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
        bool ICollection<KeyValuePair<TKey, TItem>>.Remove(KeyValuePair<TKey, TItem> item)
        {
            return Remove(item.Key);
        }

        /// <summary>
        /// Get the number of keys in the dictionary.
        /// </summary>
        public int Count
        {
            get
            {
                return _count;
            }
        }

        bool ICollection<KeyValuePair<TKey, TItem>>.IsReadOnly
        {
            get
            {
                return false;
            }
        }
        #endregion

        #region IDictionary implementation
        /// <summary>
        /// Add the specified key and value to the dictionary.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown if the key is already present in the dictionary.</exception>
        public void Add(TKey key, TItem value)
        {
            Shield.AssertInTransaction();
            if (ContainsKey(key))
                throw new ArgumentException("The given key is already present in the dictionary.");
            this[key] = value;
        }

        /// <summary>
        /// Check if the dictionary contains the given key.
        /// </summary>
        public bool ContainsKey(TKey key)
        {
            ItemKeeper v;
            if (!Shield.IsInTransaction)
                return _dict.TryGetValue(key, out v) && v != null && !v.Empty;

            v = Read(key);
            return v != null && !v.Empty;
        }

        /// <summary>
        /// Remove the specified key from the collection.
        /// </summary>
        public bool Remove(TKey key)
        {
            Shield.AssertInTransaction();
            var keeper = PrepareWrite(key);
            if (keeper != null)
            {
                if (keeper.Empty)
                    return false;
                keeper.Empty = true;
                _count.Commute((ref int n) => n--);
                return true;
            }
            var old = CurrentTransactionOldValue(key);
            if (old == null || old.Empty)
                return false;
            var locals = _localDict.Value;
            locals.Items[key] = new ItemKeeper { Empty = true };
            locals.HasChanges = true;
            _count.Commute((ref int n) => n--);
            return true;
        }

        /// <summary>
        /// Safe read based on the key - returns true if the key is present, and
        /// then also returns the value stored under that key through the out parameter.
        /// </summary>
        public bool TryGetValue(TKey key, out TItem value)
        {
            ItemKeeper v;
            if (!Shield.IsInTransaction)
            {
                if (_dict.TryGetValue(key, out v) && !v.Empty)
                {
                    value = v.Value;
                    return true;
                }
                value = default(TItem);
                return false;
            }

            v = Read(key);
            if (v != null && !v.Empty)
            {
                value = v.Value;
                return true;
            }
            value = default(TItem);
            return false;
        }

        /// <summary>
        /// Get a collection of all the keys in the dictionary. Can be used out of transactions.
        /// The result is a copy, it will not be updated if the dictionary is later changed.
        /// </summary>
        public ICollection<TKey> Keys
        {
            get
            {
                return Shield.InTransaction(
                    () => ((IEnumerable<KeyValuePair<TKey, TItem>>)this)
                        .Select(kvp => kvp.Key)
                        .ToList());
            }
        }

        /// <summary>
        /// Get a collection of all the values in the dictionary. Can be used out of transactions.
        /// The result is a copy, it will not be updated if the dictionary is later changed.
        /// </summary>
        public ICollection<TItem> Values
        {
            get
            {
                return Shield.InTransaction(
                    () => ((IEnumerable<KeyValuePair<TKey, TItem>>)this)
                        .Select(kvp => kvp.Value)
                        .ToList());
            }
        }

        #endregion

    }
}
