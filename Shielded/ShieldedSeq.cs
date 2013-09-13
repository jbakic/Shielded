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
    public class ShieldedSeq<T> : IEnumerable<T>
    {
        private class ItemKeeper
        {
            public T Value;
            public readonly Shielded<ItemKeeper> Next;

            public ItemKeeper(ItemKeeper initialNext)
            {
                Next = new Shielded<ItemKeeper>(initialNext);
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
                item = new ItemKeeper(item)
                {
                    Value = items[i]
                };
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
                return _head.Read != null;
            }
        }

        public void Prepend(T val)
        {
            var keeper = new ItemKeeper(_head)
            {
                Value = val
            };
            _head.Assign(keeper);
            if (_tail.Read == null)
                _tail.Assign(keeper);
            _count.Commute((ref int c) => c++);
        }

        public T TakeHead()
        {
            var item = _head.Read;
            if (item == null)
                throw new InvalidOperationException();
            _head.Assign(item.Next);
            // NB we don't read the tail if not needed!
            if (_head.Read == null)
                _tail.Assign(null);
            _count.Commute((ref int c) => c--);
            return item.Value;
        }

        /// <summary>
        /// Appends the specified val, commutatively - if you don't / haven't read the
        /// seq in this transaction, this will not cause conflicts.
        /// </summary>
        public void Append(T val)
        {
            var newItem = new ItemKeeper(null)
            {
                Value = val
            };
            Shield.EnlistCommute(() =>
            {
                if (_head.Read == null)
                {
                    _head.Assign(newItem);
                    _tail.Assign(newItem);
                }
                else
                {
                    _tail.Read.Next.Assign(newItem);
                    _tail.Assign(newItem);
                }
                // in case the big one degenerates, this one does not necessarily have to.
                _count.Commute((ref int c) => c++);
            }, _head, _tail, _count); // the commute degenerates if you read from the seq..
        }

        private Shielded<ItemKeeper> RefToIndex(int index)
        {
            return Shield.InTransaction(() => {
                if (index < 0 || index >= _count)
                    throw new IndexOutOfRangeException();
                var curr = _head;
                for (; index > 0; index--)
                    curr = curr.Read.Next;
                return curr;
            });
        }

        public T this [int index]
        {
            get
            {
                return RefToIndex(index).Read.Value;
            }
            set
            {
                Shield.AssertInTransaction();
                // to make this op transactional, we must create a new ItemKeeper and
                // insert him in the list.
                var refInd = RefToIndex(index);
                var newItem = new ItemKeeper(refInd.Read.Next)
                {
                    Value = value
                };
                if (_tail.Read == refInd.Read)
                    _tail.Assign(newItem);
                refInd.Assign(newItem);
            }
        }

        public int Count
        {
            get
            {
                return _count;
            }
        }

        public void RemoveAll(Func<T, bool> condition)
        {
            Shield.AssertInTransaction();
            var curr = _head;
            int removed = 0;
            while (curr.Read != null)
            {
                if (condition(curr.Read.Value))
                {
                    removed++;
                    if (_tail.Read == curr.Read)
                    {
                        _tail.Assign(null);
                        if (curr == _head)
                            _head.Assign(null);
                        break;
                    }
                    curr.Assign(curr.Read.Next);
                }
                else
                    curr = curr.Read.Next;
            }
            if (removed > 0)
                _count.Commute((ref int c) => c -= removed);
        }

        public void Clear()
        {
            _head.Assign(null);
            _tail.Assign(null);
            _count.Assign(0);
        }

        public void RemoveAt(int index)
        {
            Shield.AssertInTransaction();
            if (index < 0 || index >= _count)
                throw new IndexOutOfRangeException();
            if (index == 0)
            {
                _head.Modify((ref ItemKeeper h) => h = h.Next);
                if (_head.Read == null)
                    _tail.Assign(null);
            }
            else
            {
                // slightly more tricky, in case we need to change _tail
                var r = RefToIndex(index - 1);
                r.Read.Next.Modify((ref ItemKeeper n) => n = n.Next);
                if (r.Read.Next.Read == null)
                    _tail.Assign(r);
            }
            _count.Commute((ref int c) => c--);
        }

        public int IndexOf(T item, IEqualityComparer<T> comp = null)
        {
            return Shield.InTransaction(() => {
                if (comp == null)
                    comp = EqualityComparer<T>.Default;
                var curr = _head;
                int i = 0;
                while (curr.Read != null && !comp.Equals(curr.Read.Value, item))
                {
                    i++;
                    curr = curr.Read.Next;
                }
                return curr.Read == null ? -1 : i;
            });
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            Shield.AssertInTransaction();
            var curr = _head;
            while (curr.Read != null)
            {
                yield return curr.Read.Value;
                curr = curr.Read.Next;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<T>)this).GetEnumerator();
        }
    }
}

