using System;
using System.Collections.Generic;
using System.Linq;

namespace Shielded
{
    /// <summary>
    /// A transactional dictionary - adding, removing, replacing items are transactional,
    /// but anything done to the items is not unless they are Shielded themselves.
    /// Enumerating over this dictionary is safe, but it is a very conflicting operation.
    /// Any add/remove committed in parallel will cause an enumerating transaction to
    /// rollback. For a faster variant, but without a counter and less safe for enumerating,
    /// see <see cref="ShieldedDictNc&lt;TKey, TItem&gt;"/>.
    /// </summary>
    public class ShieldedDict<TKey, TItem> : IDictionary<TKey, TItem>
    {
        private readonly ShieldedDictNc<TKey, TItem> _dict;
        private readonly Shielded<int> _count;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="items">Initial items.</param>
        /// <param name="owner">If this is given, then in WhenCommitting subscriptions
        /// this shielded will report its owner instead of itself.</param>
        /// <param name="comparer">Equality comparer for keys.</param>
        public ShieldedDict(IEnumerable<KeyValuePair<TKey, TItem>> items = null, object owner = null,
            IEqualityComparer<TKey> comparer = null)
        {
            owner = owner ?? this;
            int count;
            _dict = new ShieldedDictNc<TKey, TItem>(items, owner, comparer, out count);
            _count = new Shielded<int>(count, owner);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="comparer">Equality comparer for keys.</param>
        public ShieldedDict(IEqualityComparer<TKey> comparer)
            : this(null, null, comparer) { }

        /// <summary>
        /// An enumerable of keys for which the current transaction made changes in the dictionary.
        /// Safely accessible from <see cref="Shield.WhenCommitting"/> subscriptions. NB that
        /// this also includes keys which were removed from the dictionary.
        /// </summary>
        public IEnumerable<TKey> Changes
        {
            get
            {
                return _dict.Changes;
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
                return _dict.Reads;
            }
        }

        #region IDictionary implementation

        /// <summary>
        /// Add the specified key and value to the dictionary.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown if the key is already present in the dictionary.</exception>
        public void Add(TKey key, TItem value)
        {
            if (!_dict.ContainsKey(key))
                _count.Commute((ref int c) => c++);
            _dict.Add(key, value);
        }

        /// <summary>
        /// Check if the dictionary contains the given key.
        /// </summary>
        public bool ContainsKey(TKey key)
        {
            return _dict.ContainsKey(key);
        }

        /// <summary>
        /// Remove the specified key from the collection.
        /// </summary>
        public bool Remove(TKey key)
        {
            if (_dict.Remove(key))
            {
                _count.Commute((ref int c) => c--);
                return true;
            }
            return false;
        }

        /// <summary>
        /// Safe read based on the key - returns true if the key is present, and
        /// then also returns the value stored under that key through the out parameter.
        /// </summary>
        public bool TryGetValue(TKey key, out TItem value)
        {
            return _dict.TryGetValue(key, out value);
        }

        /// <summary>
        /// Get or set the value under the specified key.
        /// </summary>
        /// <param name="key">Key.</param>
        public TItem this[TKey key]
        {
            get
            {
                return _dict[key];
            }
            set
            {
                if (!_dict.ContainsKey(key))
                    _count.Commute((ref int c) => c++);
                _dict[key] = value;
            }
        }

        /// <summary>
        /// Get a collection of all the keys in the dictionary. Can be used out of transactions.
        /// The result is a copy, it will not be updated if the dictionary is later changed.
        /// </summary>
        public ICollection<TKey> Keys
        {
            get
            {
                return _dict.Keys;
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
                return _dict.Values;
            }
        }

        #endregion

        #region ICollection implementation

        void ICollection<KeyValuePair<TKey, TItem>>.Add(KeyValuePair<TKey, TItem> item)
        {
            Add(item.Key, item.Value);
        }

        /// <summary>
        /// Removes all items, but one by one.
        /// </summary>
        public void Clear()
        {
            _dict.Clear();
            _count.Value = 0;
        }

        bool ICollection<KeyValuePair<TKey, TItem>>.Contains(KeyValuePair<TKey, TItem> item)
        {
            return _dict.ContainsKey(item.Key);
        }

        void ICollection<KeyValuePair<TKey, TItem>>.CopyTo(KeyValuePair<TKey, TItem>[] array, int arrayIndex)
        {
            _dict.CopyTo(array, arrayIndex);
        }

        bool ICollection<KeyValuePair<TKey, TItem>>.Remove(KeyValuePair<TKey, TItem> item)
        {
            return Remove(item.Key);
        }

        /// <summary>
        /// Gets the number of items in the dictionary.
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

        #region IEnumerable implementation

        /// <summary>
        /// Get an enumerator for the dictionary contents. Iterating over this dictionary
        /// conflicts with transactions that are adding items to the dictionary, and is
        /// fully safe.
        /// </summary>
        public IEnumerator<KeyValuePair<TKey, TItem>> GetEnumerator()
        {
            int a = _count; // just to force a conflict with any transaction changing the dict
            return _dict.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            int a = _count; // just to force a conflict with any transaction changing the dict
            return _dict.GetEnumerator();
        }

        #endregion
    }
}

