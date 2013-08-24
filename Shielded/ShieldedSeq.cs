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
        }

        public ShieldedSeq()
        {
            _head = new Shielded<ItemKeeper>();
            _tail = new Shielded<ItemKeeper>();
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
        }

        public T TakeHead()
        {
            var item = _head.Read;
            if (item == null)
                throw new InvalidOperationException();
            _head.Assign(item.Next);
            if (_tail.Read == item)
                _tail.Assign(null);
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
            }, _head, _tail); // the commute degenerates if you read from the seq..
        }

        private Shielded<ItemKeeper> RefToIndex(int index)
        {
            if (index < 0 || _head.Read == null)
                throw new IndexOutOfRangeException();
            var curr = _head;
            for (int i = 0; i < index; i++)
                if (curr.Read == null)
                    throw new IndexOutOfRangeException();
                else
                    curr = curr.Read.Next;
            if (curr.Read == null)
                throw new IndexOutOfRangeException();
            return curr;
        }

        public T this [int index]
        {
            get
            {
                return RefToIndex(index).Read.Value;
            }
            set
            {
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

        public void RemoveAt(int index)
        {
            var r = RefToIndex(index);
            r.Assign(r.Read.Next);
        }

        public int IndexOf(T item, IEqualityComparer<T> comp = null)
        {
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
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            var a = Shield.CurrentTransactionStartStamp;
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

