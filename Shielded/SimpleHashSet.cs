using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

namespace Shielded
{
    internal class SimpleHashSet : ISet<IShielded>
    {
        private int _count;

        private const int SizeShift = 3;
        private const int InitSize = 1 << SizeShift;

        private int _mask;
        private IShielded[] _array;
        private int _bloom;

        public SimpleHashSet()
        {
            _mask = InitSize - 1;
            _array = new IShielded[InitSize];
        }

        private SimpleHashSet(SimpleHashSet template)
        {
            _count = template.Count;
            _mask = template._mask;
            _array = template._array;
            _bloom = template._bloom;
        }

        bool AddInternal(IShielded item)
        {
            if (Place(item))
            {
                _bloom = _bloom | (1 << (item.GetHashCode() & 0x1F));
                if (++_count >= (_mask ^ (_mask >> 2)))
                    Increase();
                return true;
            }
            return false;
        }

        bool Place(IShielded item)
        {
            var i = item.GetHashCode() & _mask;
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
            var oldCount = _mask + 1;
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

        /// <summary>
        /// Returns a clone which actually uses the same underlying storage. This is used
        /// because the CommitCheck method grows the Enlisted set of the current transaction
        /// if that transaction had commutes. While this growing is happening, some other thread
        /// might still be checking those items for overlaps (in
        /// <see cref="CommitLocker.Enter"/>). That other thread can handle items added
        /// to the same array, but it cannot handle the array growing! (Specifically, it just needs
        /// _array to be constant.)
        /// </summary>
        /// <returns>The clone.</returns>
        public SimpleHashSet SharingClone()
        {
            return new SimpleHashSet(this);
        }

        /// <summary>
        /// Performs the commit check on the enlisted items.
        /// </summary>
        public bool CanCommit(WriteStamp ws)
        {
            for (int i = 0; i < _array.Length; i++)
                if (_array[i] != null && !_array[i].CanCommit(ws))
                    return false;
            return true;
        }

        /// <summary>
        /// Commits the enlisted items.
        /// </summary>
        public List<IShielded> Commit()
        {
            List<IShielded> changes = new List<IShielded>();
            for (int i = 0; i < _array.Length; i++)
            {
                var item = _array[i];
                if (item != null)
                {
                    if (item.HasChanges) changes.Add(item);
                    item.Commit();
                }
            }
            return changes;
        }

        /// <summary>
        /// Commits items without preparing a list of changed ones, to use
        /// when you know that there are no changes.
        /// </summary>
        public void CommitWoChanges()
        {
            for (int i = 0; i < _array.Length; i++)
                if (_array[i] != null)
                    _array[i].Commit();
        }

        /// <summary>
        /// Rolls the enlisted items back.
        /// </summary>
        public void Rollback()
        {
            for (int i = 0; i < _array.Length; i++)
                if (_array[i] != null)
                    _array[i].Rollback();
        }

        /// <summary>
        /// Helper for trimming.
        /// </summary>
        public void TrimCopies(long minOpenTransaction)
        {
            for (int i = 0; i < _array.Length; i++)
                if (_array[i] != null)
                    _array[i].TrimCopies(minOpenTransaction);
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
            var hash = item.GetHashCode();
            if (((1 << (hash & 0x1F)) & _bloom) == 0)
                return false;

            var i = hash & _mask;
            for ( ; _array[i] != null && _array[i] != item; i = (++i & _mask)) ;
            return _array[i] != null;
        }

        void ICollection<IShielded>.CopyTo(IShielded[] target, int arrayIndex)
        {
            if (_count + arrayIndex > target.Length)
                throw new IndexOutOfRangeException();
            for (int i = 0; i < _array.Length; i++)
                if (_array[i] != null)
                    target[arrayIndex++] = _array[i];
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
            var otherAsSet = other as SimpleHashSet;
            if (otherAsSet == null)
                return other.Any(Contains);
            if ((otherAsSet._bloom & this._bloom) == 0)
                return false;
            for (int i = 0; i < otherAsSet._array.Length; i++)
                if (otherAsSet._array[i] != null && Contains(otherAsSet._array[i]))
                    return true;
            return false;
        }

        public bool SetEquals(IEnumerable<IShielded> other)
        {
            var otherAsSet = other as SimpleHashSet;
            if (otherAsSet == null)
            {
                int counter = 0;
                foreach (var item in other)
                {
                    if (++counter > _count || !Contains(item))
                        return false;
                }
                return counter == _count;
            }

            if (otherAsSet._bloom != this._bloom || otherAsSet._count != _count)
                return false;
            for (int i = 0; i < otherAsSet._array.Length; i++)
                if (otherAsSet._array[i] != null && !Contains(otherAsSet._array[i]))
                    return false;
            return true;
        }

        void ISet<IShielded>.SymmetricExceptWith(IEnumerable<IShielded> other)
        {
            throw new System.NotImplementedException();
        }

        public void UnionWith(IEnumerable<IShielded> other)
        {
            var otherAsSet = other as SimpleHashSet;
            if (otherAsSet != null)
            {
                for (int i = 0; i < otherAsSet._array.Length; i++)
                    if (otherAsSet._array[i] != null)
                        AddInternal(otherAsSet._array[i]);
                return;
            }
            var otherAsList = other as List<IShielded>;
            if (otherAsList != null)
            {
                for (int i = 0; i < otherAsList.Count; i++)
                    AddInternal(otherAsList[i]);
                return;
            }
            foreach (var item in other)
                AddInternal(item);
        }
        #endregion

    }
}

