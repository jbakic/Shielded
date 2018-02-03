using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

namespace Shielded
{
#if USE_STD_HASHSET

    internal class SimpleHashSet : HashSet<IShielded>
    {
        /// <summary>
        /// Performs the commit check on the enlisted items.
        /// </summary>
        public bool CanCommit(WriteStamp ws)
        {
            foreach (var item in this)
                if (!item.CanCommit(ws))
                    return false;
            return true;
        }

        /// <summary>
        /// Commits the enlisted items.
        /// </summary>
        public List<IShielded> Commit()
        {
            List<IShielded> changes = new List<IShielded>();
            foreach (var item in this)
            {
                if (item.HasChanges)
                    changes.Add(item);
                item.Commit();
            }
            return changes;
        }

        /// <summary>
        /// Commits items without preparing a list of changed ones, to use
        /// when you know that there are no changes.
        /// </summary>
        public void CommitWoChanges()
        {
            foreach (var item in this)
                item.Commit();
        }

        /// <summary>
        /// Rolls the enlisted items back.
        /// </summary>
        public void Rollback()
        {
            foreach (var item in this)
                item.Rollback();
        }

        /// <summary>
        /// Helper for trimming.
        /// </summary>
        public void TrimCopies(long minOpenTransaction)
        {
            foreach (var item in this)
                item.TrimCopies(minOpenTransaction);
        }
    }

#else

    internal class SimpleHashSet : ICollection<IShielded>
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

        #region Partial ISet impl
        public bool Add(IShielded item)
        {
            return AddInternal(item);
        }

        public bool Overlaps(SimpleHashSet other)
        {
            if ((other._bloom & this._bloom) == 0)
                return false;
            for (int i = 0; i < other._array.Length; i++)
                if (other._array[i] != null && Contains(other._array[i]))
                    return true;
            return false;
        }

        public bool Overlaps(IShielded[] other)
        {
            for (int i = 0; i < other.Length; i++)
                if (Contains(other[i]))
                    return true;
            return false;
        }

        public bool Overlaps(IEnumerable<IShielded> other)
        {
            return other.Any(Contains);
        }

        public bool SetEquals(SimpleHashSet other)
        {
            if (other._bloom != this._bloom || other._count != _count)
                return false;
            for (int i = 0; i < other._array.Length; i++)
                if (other._array[i] != null && !Contains(other._array[i]))
                    return false;
            return true;
        }

        public void UnionWith(SimpleHashSet otherAsSet)
        {
            for (int i = 0; i < otherAsSet._array.Length; i++)
                if (otherAsSet._array[i] != null)
                    AddInternal(otherAsSet._array[i]);
        }

        public void UnionWith(IEnumerable<IShielded> other)
        {
            var otherAsSet = other as SimpleHashSet;
            if (otherAsSet != null)
            {
                UnionWith(otherAsSet);
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

#endif
}

