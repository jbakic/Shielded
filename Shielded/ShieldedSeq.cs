using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace Shielded
{
    /// <summary>
    /// Supports adding at both ends O(1), taking from head O(1), and every other op
    /// involves a search, O(n).
    /// Uses ShieldedRefs, so this class itself does not need to be shielded.
    /// </summary>
    public class ShieldedSeq<T> : IList<T>
    {
        private class ItemKeeper
        {
            public readonly T Value;
            public readonly Shielded<ItemKeeper> Next;

            public ItemKeeper(T val, ItemKeeper initialNext)
            {
                Value = val;
                Next = new Shielded<ItemKeeper>(initialNext, this);
            }

            public void ClearNext()
            {
                // this somehow fixes the leak in Queue test... cannot explain it.
                Next.Value = null;
            }
        }

        // a ShieldedRef should always be readonly! unfortunate, really. if you forget, a horrible
        // class of error becomes possible..
        private readonly Shielded<ItemKeeper> _head;
        private readonly Shielded<ItemKeeper> _tail;

        private readonly Shielded<int> _count;

        /// <summary>
        /// Initialize a new sequence with the given initial contents.
        /// </summary>
        public ShieldedSeq(params T[] items)
        {
            ItemKeeper item = null;
            for (int i = items.Length - 1; i >= 0; i--)
            {
                item = new ItemKeeper(items[i], item);
                if (_tail == null)
                    _tail = new Shielded<ItemKeeper>(item, this);
            }
            _head = new Shielded<ItemKeeper>(item, this);
            // if this is true, there were no items.
            if (_tail == null)
                _tail = new Shielded<ItemKeeper>(this);
            _count = new Shielded<int>(items.Length, this);
        }

        /// <summary>
        /// Initialize a new empty sequence.
        /// </summary>
        public ShieldedSeq()
        {
            _head = new Shielded<ItemKeeper>(this);
            _tail = new Shielded<ItemKeeper>(this);
            _count = new Shielded<int>(this);
        }

        /// <summary>
        /// Check if the sequence is non-empty. Smaller footprint than Count
        /// (reads the head only), useful in Conditionals.
        /// </summary>
        public bool HasAny
        {
            get
            {
                return _head.Value != null;
            }
        }

        /// <summary>
        /// Prepend an item, i.e. insert at index 0.
        /// </summary>
        public void Prepend(T val)
        {
            var keeper = new ItemKeeper(val, _head);
            if (_head.Value == null)
                _tail.Value = keeper;
            _head.Value = keeper;
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
                // single read => safe out of transaction.
                var h = _head.Value;
                if (h == null) throw new InvalidOperationException();
                return h.Value;
            }
        }

        /// <summary>
        /// Remove the first element of the sequence, and return it.
        /// </summary>
        public T TakeHead()
        {
            var item = _head.Value;
            if (item == null)
                throw new InvalidOperationException();
            Skip(_head);
            // NB we don't read the tail if not needed!
            if (_head.Value == null)
                _tail.Value = null;
            _count.Commute((ref int c) => c--);
            return item.Value;
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
            var newItem = new ItemKeeper(val, null);
            Shield.EnlistCommute(() => {
                if (_head.Value == null)
                {
                    _head.Value = newItem;
                    _tail.Value = newItem;
                }
                else
                {
                    _tail.Modify((ref ItemKeeper t) => {
                        t.Next.Value = newItem;
                        t = newItem;
                    });
                }
                _count.Modify((ref int c) => c++);
            }, _head, _tail, _count); // the commute degenerates if you read from the seq..
        }

        private Shielded<ItemKeeper> RefToIndex(int index)
        {
            return Shield.InTransaction(() => {
                if (index < 0 || index >= _count)
                    throw new IndexOutOfRangeException();
                var curr = _head;
                for (; index > 0; index--)
                    curr = curr.Value.Next;
                return curr;
            });
        }

        /// <summary>
        /// Get or set the item at the specified index. This iterates through the internal
        /// linked list, so it is not efficient for large sequences.
        /// </summary>
        public T this [int index]
        {
            get
            {
                return RefToIndex(index).Value.Value;
            }
            set
            {
                Shield.AssertInTransaction();
                // to make this op transactional, we must create a new ItemKeeper and
                // insert him in the list.
                var refInd = RefToIndex(index);
                var newItem = new ItemKeeper(value, refInd.Value.Next);
                if (_tail.Value == refInd.Value)
                    _tail.Value = newItem;
                refInd.Modify((ref ItemKeeper r) => {
                    r.ClearNext();
                    r = newItem;
                });
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

        private static void Skip(Shielded<ItemKeeper> sh)
        {
            sh.Modify((ref ItemKeeper c) => {
                var old = c;
                c = c.Next;
                old.ClearNext();
            });
        }

        /// <summary>
        /// Remove from the sequence all items that satisfy the given predicate.
        /// </summary>
        public void RemoveAll(Func<T, bool> condition)
        {
            Shield.AssertInTransaction();
            var curr = _head;
            var tail = _tail.Value;
            ItemKeeper previous = null;
            int removed = 0;
            try
            {
                while (curr.Value != null)
                {
                    if (condition(curr.Value.Value))
                    {
                        removed++;
                        if (tail == curr.Value)
                        {
                            _tail.Value = previous;
                            if (previous == null)
                                _head.Value = null;
                            break;
                        }
                        Skip(curr);
                    }
                    else
                    {
                        previous = curr;
                        curr = curr.Value.Next;
                    }
                }
            }
            finally
            {
                if (removed > 0)
                    _count.Commute((ref int c) => c -= removed);
            }
        }

        /// <summary>
        /// Clear the sequence.
        /// </summary>
        public void Clear()
        {
            _head.Value = null;
            _tail.Value = null;
            _count.Value = 0;
        }

        /// <summary>
        /// Remove the item at the given index.
        /// </summary>
        public void RemoveAt(int index)
        {
            Shield.AssertInTransaction();
            if (index == 0)
            {
                if (_head.Value == null)
                    throw new IndexOutOfRangeException();
                Skip(_head);
                if (_head.Value == null)
                    _tail.Value = null;
            }
            else
            {
                // slightly more tricky, in case we need to change _tail
                var r = RefToIndex(index - 1).Value;
                Skip(r.Next);
                if (r.Next.Value == null)
                    _tail.Value = r;
            }
            _count.Commute((ref int c) => c--);
        }

        /// <summary>
        /// Remove the specified item from the sequence.
        /// </summary>
        public bool Remove(T item, IEqualityComparer<T> comp = null)
        {
            Shield.AssertInTransaction();
            if (comp == null) comp = EqualityComparer<T>.Default;

            var curr = _head;
            ItemKeeper previous = null;
            while (curr.Value != null)
            {
                if (comp.Equals(item, curr.Value.Value))
                {
                    _count.Commute((ref int c) => c--);
                    if (_tail.Value == curr.Value)
                        _tail.Value = previous;
                    Skip(curr);
                    return true;
                }
                else
                {
                    previous = curr;
                    curr = curr.Value.Next;
                }
            }
            return false;
        }

        /// <summary>
        /// Search the sequence for the given item.
        /// </summary>
        /// <returns>The index of the item in the sequence.</returns>
        public int IndexOf(T item, IEqualityComparer<T> comp = null)
        {
            if (comp == null)
                comp = EqualityComparer<T>.Default;
            return Shield.InTransaction(() => {
                var curr = _head;
                int i = 0;
                while (curr.Value != null && !comp.Equals(curr.Value.Value, item))
                {
                    i++;
                    curr = curr.Value.Next;
                }
                return curr.Value == null ? -1 : i;
            });
        }

        /// <summary>
        /// Search the sequence for the given item.
        /// </summary>
        /// <returns>The index of the item in the sequence.</returns>
        public int IndexOf(T item)
        {
            return IndexOf(item, null);
        }

        /// <summary>
        /// Get an enumerator for the sequence. Although it is just a read, it must be
        /// done in a transaction since concurrent changes would make the result unstable.
        /// </summary>
        public IEnumerator<T> GetEnumerator()
        {
            Shield.AssertInTransaction();
            var curr = _head;
            while (curr.Value != null)
            {
                yield return curr.Value.Value;
                curr = curr.Value.Next;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<T>)this).GetEnumerator();
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
            Shield.InTransaction(() => {
                if (_count + arrayIndex > array.Length)
                    throw new IndexOutOfRangeException();
                foreach (var v in this)
                    array[arrayIndex++] = v;
            });
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
            if (index == _count)
            {
                Append(item);
                return;
            }

            RefToIndex(index).Modify((ref ItemKeeper r) => {
                var newItem = new ItemKeeper(item, r);
                if (r == null)
                    _tail.Value = newItem;
                r = newItem;
            });
            _count.Commute((ref int c) => c++);
        }
    }
}

