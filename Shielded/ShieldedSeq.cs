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
                Next = new Shielded<ItemKeeper>(initialNext);
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

        public ShieldedSeq(params T[] items)
        {
            ItemKeeper item = null;
            for (int i = items.Length - 1; i >= 0; i--)
            {
                item = new ItemKeeper(items[i], item);
                if (_tail == null)
                    _tail = new Shielded<ItemKeeper>(item);
            }
            _head = new Shielded<ItemKeeper>(item);
            // if this is true, there were no items.
            if (_tail == null)
                _tail = new Shielded<ItemKeeper>();
            _count = new Shielded<int>(items.Length);
        }

        public ShieldedSeq()
        {
            _head = new Shielded<ItemKeeper>();
            _tail = new Shielded<ItemKeeper>();
            _count = new Shielded<int>();
        }

        /// <summary>
        /// Smaller footprint than Count (reads the head only), useful in Conditionals.
        /// </summary>
        public bool HasAny
        {
            get
            {
                return _head.Value != null;
            }
        }

        public void Prepend(T val)
        {
            var keeper = new ItemKeeper(val, _head);
            if (_head.Value == null)
                _tail.Value = keeper;
            _head.Value = keeper;
            _count.Commute((ref int c) => c++);
        }

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
        /// Appends the specified val, commutatively - if you don't / haven't read the
        /// seq in this transaction, this will not cause conflicts.
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

        public void Clear()
        {
            _head.Value = null;
            _tail.Value = null;
            _count.Value = 0;
        }

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

        public int IndexOf(T item)
        {
            return IndexOf(item, null);
        }

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

        public void Add(T item)
        {
            Append(item);
        }

        public bool Contains(T item)
        {
            return IndexOf(item) >= 0;
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            Shield.InTransaction(() => {
                if (_count + arrayIndex > array.Length)
                    throw new IndexOutOfRangeException();
                foreach (var v in this)
                    array[arrayIndex++] = v;
            });
        }

        public bool Remove(T item)
        {
            return Remove(item, null);
        }

        public bool IsReadOnly
        {
            get
            {
                return false;
            }
        }

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

