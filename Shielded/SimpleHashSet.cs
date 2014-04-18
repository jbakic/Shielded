using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

namespace Shielded
{
    internal class SimpleHash
    {
        private static int _seed = 0;

        public static int Get()
        {
            return unchecked(Interlocked.Increment(ref _seed) * 37);
        }
    }

    internal class SimpleHashSet : ISet<IShielded>
    {
        private int _count;

        private const int SizeShift = 3;
        private const int InitSize = 1 << SizeShift;

        private int _mask = InitSize - 1;
        private IShielded[] _array = new IShielded[InitSize];

        bool AddInternal(IShielded item)
        {
            if (Place(item))
            {
                if (++_count >= (_mask ^ (_mask >> 2)))
                    Increase();
                return true;
            }
            return false;
        }

        bool Place(IShielded item)
        {
            var i = item.PseudoHash & _mask;
            for ( ; _array[i] != null && _array[i] != item; i = (++i & _mask)) ;

            if (_array[i] == null)
            {
                _array[i] = item;
                return true;
            }
            return false;
        }

        void Increase()
        {
            var oldCount = _array.Length;
            var newCount = oldCount << 1;
            var oldArray = _array;
            _array = new IShielded[newCount];
            _mask = newCount - 1;
            for (int i = 0; i < oldCount; i++)
            {
                if (oldArray[i] != null)
                    Place(oldArray[i]);
            }
        }

        #region IEnumerable implementation
        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<IShielded>)this).GetEnumerator();
        }
        #endregion

        #region IEnumerable implementation
        public IEnumerator<IShielded> GetEnumerator()
        {
            for (int i = 0; i < _array.Length; i++)
            {
                if (_array[i] != null)
                    yield return _array[i];
            }
        }
        #endregion

        #region ICollection implementation
        void ICollection<IShielded>.Add(IShielded item)
        {
            throw new System.NotImplementedException();
        }

        void ICollection<IShielded>.Clear()
        {
            throw new System.NotImplementedException();
        }

        public bool Contains(IShielded item)
        {
            var i = item.PseudoHash & _mask;
            for ( ; _array[i] != null && _array[i] != item; i = (++i & _mask)) ;
            return _array[i] != null;
        }

        void ICollection<IShielded>.CopyTo(IShielded[] array, int arrayIndex)
        {
            if (_count + arrayIndex > array.Length)
                throw new IndexOutOfRangeException();
            foreach (var item in this)
                array[arrayIndex++] = item;
        }

        bool ICollection<IShielded>.Remove(IShielded item)
        {
            throw new System.NotImplementedException();
        }

        public int Count
        {
            get
            {
                return _count;
            }
        }

        bool ICollection<IShielded>.IsReadOnly
        {
            get
            {
                return false;
            }
        }
        #endregion

        #region ISet implementation
        public bool Add(IShielded item)
        {
            return AddInternal(item);
        }

        void ISet<IShielded>.ExceptWith(IEnumerable<IShielded> other)
        {
            throw new System.NotImplementedException();
        }

        void ISet<IShielded>.IntersectWith(IEnumerable<IShielded> other)
        {
            throw new System.NotImplementedException();
        }

        bool ISet<IShielded>.IsProperSubsetOf(IEnumerable<IShielded> other)
        {
            throw new System.NotImplementedException();
        }

        bool ISet<IShielded>.IsProperSupersetOf(IEnumerable<IShielded> other)
        {
            throw new System.NotImplementedException();
        }

        bool ISet<IShielded>.IsSubsetOf(IEnumerable<IShielded> other)
        {
            throw new System.NotImplementedException();
        }

        bool ISet<IShielded>.IsSupersetOf(IEnumerable<IShielded> other)
        {
            throw new System.NotImplementedException();
        }

        public bool Overlaps(IEnumerable<IShielded> other)
        {
            return other.Any(Contains);
        }

        public bool SetEquals(IEnumerable<IShielded> other)
        {
            int counter = 0;
            foreach (var item in other)
            {
                if (++counter > _count || !Contains(item))
                    return false;
            }
            return counter == _count;
        }

        void ISet<IShielded>.SymmetricExceptWith(IEnumerable<IShielded> other)
        {
            throw new System.NotImplementedException();
        }

        public void UnionWith(IEnumerable<IShielded> other)
        {
            foreach (var item in other)
                AddInternal(item);
        }
        #endregion

    }
}

