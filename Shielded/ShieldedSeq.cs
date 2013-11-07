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

            public void ClearNext()
            {
                // this somehow fixes the leak in Queue test... cannot explain it.
                Next.Assign(null);
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
            if (_head.Read == null)
                _tail.Assign(keeper);
            _head.Assign(keeper);
            _count.Commute((ref int c) => c++);
        }

        public T Head
        {
            get
            {
                // single read => safe out of transaction.
                var h = _head.Read;
                if (h == null) throw new InvalidOperationException();
                return h.Value;
            }
        }

        public T TakeHead()
        {
            var item = _head.Read;
            if (item == null)
                throw new InvalidOperationException();
            Skip(_head);
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
            Shield.EnlistCommute(false, () => {
                if (_head.Read == null)
                {
                    _head.Assign(newItem);
                    _tail.Assign(newItem);
                }
                else
                {
                    // this cannot be a commute. if someone reads _head, he
                    // can read through the items to the last one...
                    _tail.Modify((ref ItemKeeper t) => {
                        t.Next.Assign(newItem);
                        t = newItem;
                    });
                }
            }, _head, _tail); // the commute degenerates if you read from the seq..
            _count.Commute((ref int c) => c++);
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
            ItemKeeper previous = null;
            int removed = 0;
            while (curr.Read != null)
            {
                if (condition(curr.Read.Value))
                {
                    removed++;
                    if (_tail.Read == curr.Read)
                    {
                        _tail.Assign(previous);
                        if (curr == _head)
                            _head.Assign(null);
                        break;
                    }
                    Skip(curr);
                }
                else
                {
                    previous = curr;
                    curr = curr.Read.Next;
                }
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
            if (index == 0)
            {
                if (_head.Read == null)
                    throw new IndexOutOfRangeException();
                Skip(_head);
                if (_head.Read == null)
                    _tail.Assign(null);
            }
            else
            {
                // slightly more tricky, in case we need to change _tail
                var r = RefToIndex(index - 1).Read;
                Skip(r.Next);
                if (r.Next.Read == null)
                    _tail.Assign(r);
            }
            _count.Commute((ref int c) => c--);
        }

        public bool Remove(T item, IEqualityComparer<T> comp = null)
        {
            Shield.AssertInTransaction();
            if (comp == null) comp = EqualityComparer<T>.Default;

            var curr = _head;
            ItemKeeper previous = null;
            while (curr.Read != null)
            {
                if (comp.Equals(item, curr.Read.Value))
                {
                    _count.Commute((ref int c) => c--);
                    if (_tail.Read == curr.Read)
                        _tail.Assign(previous);
                    Skip(curr);
                    return true;
                }
                else
                {
                    previous = curr;
                    curr = curr.Read.Next;
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

