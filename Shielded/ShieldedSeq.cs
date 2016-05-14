using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace Shielded
{
    /// <summary>
    /// Supports adding at both ends O(1), taking from head O(1), and every other op
    /// involves a search, O(n). Enumerating must be done in transaction, unless you
    /// read only one element, e.g. using LINQ Any or First.
    /// For a faster version, but with no Count property, see <see cref="ShieldedSeqNc&lt;T&gt;"/>.
    /// </summary>
    public class ShieldedSeq<T> : IList<T>
    {
        private readonly ShieldedSeqNc<T> _seq;
        private readonly Shielded<int> _count;

        /// <summary>
        /// Initialize a new sequence with the given initial contents, and optionally
        /// an "owner" - this seq will then report that object to WhenSubmitting subscriptions.
        /// </summary>
        public ShieldedSeq(T[] items = null, object owner = null)
        {
            owner = owner ?? this;
            _seq = new ShieldedSeqNc<T>(items, owner);
            _count = new Shielded<int>(items == null ? 0 : items.Length, owner);
        }

        /// <summary>
        /// Initialize a new sequence with the given initial contents.
        /// </summary>
        public ShieldedSeq(params T[] items) : this(items, null) { }

        /// <summary>
        /// Prepend an item, i.e. insert at index 0.
        /// </summary>
        public void Prepend(T val)
        {
            _seq.Prepend(val);
            _count.Commute((ref int c) => c++);
        }

        /// <summary>
        /// Peek at the first element. Throws <see cref="InvalidOperationException"/> if
        /// there is none.
        /// </summary>
        public T Head
        {
            get
            {
                return _seq.Head;
            }
        }

        /// <summary>
        /// Remove the first element of the sequence, and return it.
        /// </summary>
        public T TakeHead()
        {
            var h = _seq.TakeHead();
            _count.Commute((ref int c) => c--);
            return h;
        }

        /// <summary>
        /// Remove and yield elements from the head of the sequence.
        /// </summary>
        public IEnumerable<T> Consume
        {
            get
            {
                foreach (var a in _seq.Consume)
                {
                    _count.Commute((ref int c) => c--);
                    yield return a;
                }
            }
        }

        /// <summary>
        /// Append the specified value, commutatively - if you don't / haven't touched the
        /// sequence in this transaction (using other methods/properties), this will not cause
        /// conflicts! Effectively, the value is simply appended to whatever the sequence
        /// is at commit time. Multiple calls to Append made in one transaction will
        /// append the items in that order - they commute with other transactions only.
        /// </summary>
        public void Append(T val)
        {
            _seq.Append(val);
            _count.Commute((ref int c) => c++);
        }

        /// <summary>
        /// Get or set the item at the specified index. This iterates through the internal
        /// linked list, so it is not efficient for large sequences.
        /// </summary>
        public T this [int index]
        {
            get
            {
                return _seq[index];
            }
            set
            {
                _seq[index] = value;
            }
        }

        /// <summary>
        /// Get the number of items in this sequence.
        /// </summary>
        public int Count
        {
            get
            {
                return _count;
            }
        }

        /// <summary>
        /// Remove from the sequence all items that satisfy the given predicate.
        /// </summary>
        public void RemoveAll(Func<T, bool> condition)
        {
            int removed = 0;
            try
            {
                _seq.RemoveAll(condition, out removed);
            }
            finally
            {
                _count.Commute((ref int c) => c -= removed);
            }
        }

        /// <summary>
        /// Clear the sequence.
        /// </summary>
        public void Clear()
        {
            _seq.Clear();
            _count.Value = 0;
        }

        /// <summary>
        /// Remove the item at the given index.
        /// </summary>
        public void RemoveAt(int index)
        {
            _seq.RemoveAt(index);
            _count.Commute((ref int c) => c--);
        }

        /// <summary>
        /// Remove the specified item from the sequence.
        /// </summary>
        public bool Remove(T item, IEqualityComparer<T> comp = null)
        {
            if (_seq.Remove(item, comp))
            {
                _count.Commute((ref int c) => c--);
                return true;
            }
            return false;
        }

        /// <summary>
        /// Search the sequence for the given item.
        /// </summary>
        /// <returns>The index of the item in the sequence.</returns>
        public int IndexOf(T item, IEqualityComparer<T> comp = null)
        {
            return _seq.IndexOf(item, comp);
        }

        /// <summary>
        /// Search the sequence for the given item.
        /// </summary>
        /// <returns>The index of the item in the sequence.</returns>
        public int IndexOf(T item)
        {
            return _seq.IndexOf(item, null);
        }

        /// <summary>
        /// Get an enumerator for the sequence. Although it is just a read, it must be
        /// done in a transaction since concurrent changes would make the result unstable.
        /// </summary>
        public IEnumerator<T> GetEnumerator()
        {
            return _seq.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _seq.GetEnumerator();
        }

        /// <summary>
        /// Add the specified item to the end of the sequence. Same as <see cref="Append"/>,
        /// so, it's commutable.
        /// </summary>
        public void Add(T item)
        {
            Append(item);
        }

        /// <summary>
        /// Check if the sequence contains a given item.
        /// </summary>
        public bool Contains(T item)
        {
            return IndexOf(item) >= 0;
        }

        /// <summary>
        /// Copy the sequence to an array.
        /// </summary>
        /// <param name="array">The array to copy to.</param>
        /// <param name="arrayIndex">Index in the array where to begin the copy.</param>
        public void CopyTo(T[] array, int arrayIndex)
        {
            _seq.CopyTo(array, arrayIndex);
        }

        /// <summary>
        /// Remove the specified item from the sequence.
        /// </summary>
        public bool Remove(T item)
        {
            return Remove(item, null);
        }

        bool ICollection<T>.IsReadOnly
        {
            get
            {
                return false;
            }
        }

        /// <summary>
        /// Insert an item at the specified index.
        /// </summary>
        public void Insert(int index, T item)
        {
            _seq.Insert(index, item);
            _count.Commute((ref int c) => c++);
        }
    }
}

