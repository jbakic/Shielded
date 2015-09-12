using System;
using System.Linq;
using System.Linq.Expressions;
using System.Collections;
using System.Collections.Generic;

namespace Shielded
{
    /// <summary>
    /// A shielded red-black tree, for keeping sorted data. Each node is a
    /// Shielded struct, so parallel operations are possible. Multiple items
    /// may be added under the same key.
    /// </summary>
    public class ShieldedTree<TKey, TValue> : ShieldedTreeNc<TKey, TValue>, IDictionary<TKey, TValue>
    {
        private Shielded<int> _count;

        /// <summary>
        /// Initializes a new tree, which will use a given comparer, or using the .NET default
        /// comparer if none is specified.
        /// </summary>
        public ShieldedTree(IComparer<TKey> comparer = null, object owner = null)
            : base(comparer, owner)
        {
            _count = new Shielded<int>(owner ?? this);
        }

        /// <summary>
        /// Called after inserting a new node.
        /// </summary>
        protected override void OnInsert()
        {
            _count.Commute((ref int c) => c++);
        }

        /// <summary>
        /// Called after a node is removed from the tree.
        /// </summary>
        protected override void OnRemove()
        {
            _count.Commute((ref int c) => c--);
        }

        /// <summary>
        /// Clear this instance. Efficient, O(1).
        /// </summary>
        public override void Clear()
        {
            base.Clear();
            _count.Value = 0;
        }

        void ICollection<KeyValuePair<TKey, TValue>>.Add(KeyValuePair<TKey, TValue> item)
        {
            Add(item.Key, item.Value);
        }

        /// <summary>
        /// Gets the number of items in the tree.
        /// </summary>
        public int Count
        {
            get
            {
                return _count;
            }
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.IsReadOnly
        {
            get
            {
                return false;
            }
        }
    }
}

