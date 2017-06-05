using System;
using System.Linq;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Shielded
{
    /// <summary>
    /// A transactional dictionary - adding, removing, replacing items are transactional,
    /// but anything done to the items is not unless they are Shielded themselves. The 'Nc' stands
    /// for 'no count' - you cannot find out how many items it has, unless you enumerate.
    /// WARNING: Enumerating transactions will not conflict with parallel transactions which
    /// add new items to the dictionary. This is OK for read-only transactions, but writers
    /// might successfully commit some result even though they did not see all items.
    /// For fully safe enumerating, please use <see cref="ShieldedDict&lt;TKey, TItem&gt;"/>.
    /// </summary>
    public class ShieldedDictNc<TKey, TItem> : IShielded, IEnumerable<KeyValuePair<TKey, TItem>>
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
            public bool HasChanges;
            public bool Locked;

            public LocalDict(IEqualityComparer<TKey> comparer)
            {
                Items = new Dictionary<TKey, ItemKeeper>(comparer);
            }
        }

        private readonly IEqualityComparer<TKey> _comparer;
        private readonly ConcurrentDictionary<TKey, ItemKeeper> _dict;
        private readonly ConcurrentDictionary<TKey, WriteStamp> _writeStamps;
        private readonly TransactionalStorage<LocalDict> _localDict = new TransactionalStorage<LocalDict>();
        private readonly object _owner;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="items">Initial items.</param>
        /// <param name="owner">If this is given, then in WhenCommitting subscriptions
        /// this shielded will report its owner instead of itself.</param>
        /// <param name="comparer">Equality comparer for keys.</param>
        public ShieldedDictNc(IEnumerable<KeyValuePair<TKey, TItem>> items = null, object owner = null,
            IEqualityComparer<TKey> comparer = null)
        {
            _comparer = comparer ?? EqualityComparer<TKey>.Default;
            _dict = items == null ? new ConcurrentDictionary<TKey, ItemKeeper>(_comparer) :
                new ConcurrentDictionary<TKey, ItemKeeper>(
                    items.Select(kvp =>
                        new KeyValuePair<TKey, ItemKeeper>(kvp.Key, new ItemKeeper() { Value = kvp.Value })), _comparer);
            _writeStamps = new ConcurrentDictionary<TKey, WriteStamp>(_comparer);
            _owner = owner ?? this;
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="comparer">Equality comparer for keys.</param>
        public ShieldedDictNc(IEqualityComparer<TKey> comparer)
            : this(null, null, comparer) { }

        internal ShieldedDictNc(IEnumerable<KeyValuePair<TKey, TItem>> items, object owner,
            IEqualityComparer<TKey> comparer, out int count)
            : this(items, owner, comparer)
        {
            count = _dict.Count;
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

            WriteStamp w;
            if (_writeStamps.TryGetValue(key, out w) && w.Locked && w.Version <= Shield.ReadStamp)
                w.Wait();
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
                _localDict.Value = locals = new LocalDict(_comparer);
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
            if (!(hasLocal = locals.Items.TryGetValue(key, out v)) || v == null || Shield.ReadingOldState)
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
                if (_dict.TryGetValue(key, out curr) && curr.Version > Shield.ReadStamp)
                    throw new TransException("Write collision.");

                if (local == null)
                {
                    var locals = _localDict.Value;
                    locals.Items[key] = new ItemKeeper() { Value = value };
                    locals.HasChanges = true;
                }
                else
                {
                    local.Empty = false;
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

        /// <summary>
        /// An enumerable of keys which the current transaction read or wrote into.
        /// Safely accessible from <see cref="Shield.WhenCommitting"/> subscriptions. NB that
        /// this also includes keys which were removed from the dictionary.
        /// </summary>
        public IEnumerable<TKey> Reads
        {
            get
            {
                if (Shield.IsInTransaction && _localDict.HasValue)
                    return _localDict.Value.Items.Keys;
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
                return _owner;
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
                var ctx = Shield.Context;
                foreach (var kvp in locals.Items)
                {
                    if (kvp.Value != null &&
                        _writeStamps.TryGetValue(kvp.Key, out ws) && ws.Locker == ctx)
                    {
                        _writeStamps.TryRemove(kvp.Key, out ws);
                    }
                }
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

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<KeyValuePair<TKey, TItem>>)this).GetEnumerator();
        }

        /// <summary>
        /// Get an enumerator for the dictionary contents. Iterating over this dictionary
        /// does not conflict with other transactions that are adding new items, so it
        /// is not fully safe. For read-only transactions, however, no problem.
        /// </summary>
        public IEnumerator<KeyValuePair<TKey, TItem>> GetEnumerator()
        {
            Shield.AssertInTransaction();
            var keys = _localDict.HasValue ?
                _dict.Keys.Union(_localDict.Value.Items.Keys, _comparer) : _dict.Keys;
            foreach (var key in keys)
            {
                var v = Read(key);
                if (v == null || v.Empty) continue;
                yield return new KeyValuePair<TKey, TItem>(key, v.Value);
            }
        }

        /// <summary>
        /// Removes all items, but one by one.
        /// </summary>
        public void Clear()
        {
            foreach (var key in Keys)
                Remove(key);
        }

        /// <summary>
        /// Copy the dictionary contents to an array. Involves enumerating, might not get all
        /// items which should have been visible at the time of your commit. This does not
        /// affect read-only transactions, or those writers that do not logically depend on
        /// seeing all items.
        /// </summary>
        public void CopyTo(KeyValuePair<TKey, TItem>[] array, int arrayIndex)
        {
            Shield.InTransaction(() => {
                foreach (KeyValuePair<TKey, TItem> kvp in this)
                    array[arrayIndex++] = kvp;
            });
        }

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
                var res = !keeper.Empty;
                keeper.Empty = true;
                return res;
            }
            var old = CurrentTransactionOldValue(key);
            if (old == null || old.Empty)
                return false;
            var locals = _localDict.Value;
            locals.Items[key] = new ItemKeeper { Empty = true };
            locals.HasChanges = true;
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
        /// Like all enumerating operations on this type, it is not completely safe, unless
        /// your transaction is read-only.
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
        /// Like all enumerating operations on this type, it is not completely safe, unless
        /// your transaction is read-only.
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
    }
}
