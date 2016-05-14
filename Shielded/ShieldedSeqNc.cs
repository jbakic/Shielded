using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;

namespace Shielded
{
    /// <summary>
    /// Supports adding at both ends O(1), taking from head O(1), and every other op
    /// involves a search, O(n). Enumerating must be done in transaction, unless you
    /// read only one element, e.g. using LINQ Any or First.
    /// The 'Nc' stands for 'no count' - the number of items can only be obtained by
    /// enumerating. This enables it to be faster in other ops, since counters are
    /// very contentious.
    /// For a sequence with a counter, see <see cref="ShieldedSeq&lt;T&gt;"/>.
    /// </summary>
    public class ShieldedSeqNc<T> : IEnumerable<T>
    {
        private class ItemKeeper
        {
            public readonly T Value;
            public readonly Shielded<ItemKeeper> Next;

            public ItemKeeper(T val, ItemKeeper next, object owner)
            {
                Value = val;
                Next = new Shielded<ItemKeeper>(next, owner);
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
        private readonly object _owner;

        private Shielded<ItemKeeper> CreateRef(ItemKeeper item = null)
        {
            return new Shielded<ItemKeeper>(item, _owner);
        }

        /// <summary>
        /// Initialize a new sequence with the given initial contents.
        /// </summary>
        /// <param name="items">Initial items.</param>
        /// <param name="owner">If this is given, then in WhenCommitting subscriptions
        /// this shielded will report its owner instead of itself.</param>
        public ShieldedSeqNc(T[] items = null, object owner = null)
        {
            _owner = owner ?? this;
            if (items == null)
            {
                _head = CreateRef();
                _tail = CreateRef();
                return;
            }

            ItemKeeper item = null;
            for (int i = items.Length - 1; i >= 0; i--)
            {
                item = new ItemKeeper(items[i], item, _owner);
                if (_tail == null)
                    _tail = CreateRef(item);
            }
            _head = CreateRef(item);
            // if this is true, there were no items.
            if (_tail == null)
                _tail = CreateRef();
        }

        /// <summary>
        /// Initialize a new sequence with the given initial contents.
        /// </summary>
        public ShieldedSeqNc(params T[] items) : this(items, null) { }

        /// <summary>
        /// Prepend an item, i.e. insert at index 0.
        /// </summary>
        public void Prepend(T val)
        {
            var keeper = new ItemKeeper(val, _head, _owner);
            if (_head.Value == null)
                _tail.Value = keeper;
            _head.Value = keeper;
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
            return Consume.Take(1).Single();
        }

        /// <summary>
        /// Remove and yield elements from the head of the sequence.
        /// </summary>
        public IEnumerable<T> Consume
        {
            get
            {
                while (true)
                {
                    var item = _head.Value;
                    if (item == null)
                        yield break;
                    Skip(_head);
                    // NB we don't read the tail if not needed!
                    if (_head.Value == null)
                        _tail.Value = null;
                    yield return item.Value;
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
            var newItem = new ItemKeeper(val, null, _owner);
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
            }, _head, _tail); // the commute degenerates if you read from the seq..
        }

        private Shielded<ItemKeeper> RefToIndex(int index, bool plusOne = false)
        {
            return Shield.InTransaction(() => {
                if (index < 0)
                    throw new IndexOutOfRangeException();
                var curr = _head;
                for (; index > 0; index--)
                {
                    if (curr.Value == null)
                        throw new IndexOutOfRangeException();
                    curr = curr.Value.Next;
                }
                if (!plusOne && curr.Value == null)
                    throw new IndexOutOfRangeException();
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
                RefToIndex(index).Modify((ref ItemKeeper r) => { 
                    var newItem = new ItemKeeper(value, r.Next, _owner);
                    if (r.Next.Value == null)
                        _tail.Value = newItem;
                    r.ClearNext();
                    r = newItem;
                });
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
            int _;
            RemoveAll(condition, out _);
        }

        /// <summary>
        /// Remove from the sequence all items that satisfy the given predicate.
        /// Returns the number of removed items in an out param, guaranteed to
        /// equal actual number of removed items even if the condition lambda throws.
        /// </summary>
        public void RemoveAll(Func<T, bool> condition, out int removed)
        {
            Shield.AssertInTransaction();
            var curr = _head;
            var tail = _tail.Value;
            ItemKeeper previous = null;
            removed = 0;
            while (curr.Value != null)
            {
                if (condition(curr.Value.Value))
                {
                    removed++;
                    if (tail == curr.Value)
                    {
                        _tail.Value = previous;
                        if (previous == null)
                        {
                            _head.Value = null;
                            break;
                        }
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

        /// <summary>
        /// Clear the sequence.
        /// </summary>
        public void Clear()
        {
            _head.Value = null;
            _tail.Value = null;
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
                    if (curr.Value.Next.Value == null)
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
        /// <returns>The index of the item in the sequence, or -1 if not found.</returns>
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
        /// Get an enumerator for the sequence. Although it is just a read, it must be
        /// done in a transaction since concurrent changes would make the result unstable.
        /// However, if you just read the first item (e.g. by calling Any or First), that
        /// will work out of transaction too.
        /// </summary>
        public IEnumerator<T> GetEnumerator()
        {
            var curr = _head.Value;
            if (curr == null)
                yield break;
            yield return curr.Value;
            Shield.AssertInTransaction();
            curr = curr.Next;
            while (curr != null)
            {
                yield return curr.Value;
                curr = curr.Next;
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
                foreach (var v in this)
                    array[arrayIndex++] = v;
            });
        }

        /// <summary>
        /// Insert an item at the specified index.
        /// </summary>
        public void Insert(int index, T item)
        {
            RefToIndex(index, plusOne: true).Modify((ref ItemKeeper r) => {
                var newItem = new ItemKeeper(item, r, _owner);
                if (r == null)
                    _tail.Value = newItem;
                r = newItem;
            });
        }
    }
}

