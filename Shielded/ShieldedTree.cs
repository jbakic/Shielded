using System;
using System.Linq;
using System.Linq.Expressions;
using System.Collections;
using System.Collections.Generic;

namespace Shielded
{
    /// <summary>
    /// A shielded red-black tree, for keeping sorted data. Each node is a
    /// Shielded struct, so parallel operations are possible. Multiple items
    /// may be added under the same key.
    /// </summary>
    public class ShieldedTree<TKey, TValue> : IDictionary<TKey, TValue> where TValue : class
    {
        private enum Color
        {
            Red = 0,
            Black
        }

        private struct Node
        {
            public Color Color;
            public TKey Key;
            public TValue Value;
            public Shielded<Node> Left;
            public Shielded<Node> Right;
            public Shielded<Node> Parent;
        }

        private void ClearLinks(Shielded<Node> node)
        {
            node.Modify((ref Node n) => n.Left = n.Right = n.Parent = null);
        }

        private readonly Shielded<Shielded<Node>> _head;
        private readonly Shielded<int> _count;
        private readonly IComparer<TKey> _comparer;

        /// <summary>
        /// Initializes a new tree, which will use a given comparer, or using the .NET default
        /// comparer if none is specified.
        /// </summary>
        public ShieldedTree(IComparer<TKey> comparer = null)
        {
            _head = new Shielded<Shielded<Node>>(this);
            _count = new Shielded<int>(this);
            _comparer = comparer != null ? comparer : Comparer<TKey>.Default;
        }

        private Shielded<Node> FindInternal(TKey key)
        {
            return Shield.InTransaction(() => {
                var curr = _head.Value;
                int comparison;
                while (curr != null &&
                       (comparison = _comparer.Compare(curr.Value.Key, key)) != 0)
                {
                    if (comparison > 0)
                        curr = curr.Value.Left;
                    else
                        curr = curr.Value.Right;
                }
                return curr;
            });
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<KeyValuePair<TKey, TValue>>)this).GetEnumerator();
        }

        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
        {
            Shield.AssertInTransaction();
            Stack<Shielded<Node>> centerStack = new Stack<Shielded<Node>>();
            var curr = _head.Value;
            while (curr != null)
            {
                while (curr.Value.Left != null)
                {
                    centerStack.Push(curr);
                    curr = curr.Value.Left;
                }

                yield return new KeyValuePair<TKey, TValue>(curr.Value.Key, curr.Value.Value);

                while (curr.Value.Right == null && centerStack.Count > 0)
                {
                    curr = centerStack.Pop();
                    yield return new KeyValuePair<TKey, TValue>(curr.Value.Key, curr.Value.Value);
                }
                curr = curr.Value.Right;
            }
        }

        /// <summary>
        /// Enumerate all key-value pairs, whose keys are in the given range. The range
        /// is inclusive, both from and to are included in the result (if the tree contains
        /// those keys). The items are returned sorted.
        /// </summary>
        public IEnumerable<KeyValuePair<TKey, TValue>> Range(TKey from, TKey to)
        {
            foreach (var n in RangeInternal(from, to))
                yield return new KeyValuePair<TKey, TValue>(n.Value.Key, n.Value.Value);
        }

        /// <summary>
        /// Enumerates only over the nodes in the range. borders included.
        /// </summary>
        private IEnumerable<Shielded<Node>> RangeInternal(TKey from, TKey to)
        {
            if (_comparer.Compare(from, to) > 0)
                yield break;
            Shield.AssertInTransaction();
            Stack<Shielded<Node>> centerStack = new Stack<Shielded<Node>>();
            var curr = _head.Value;
            while (curr != null)
            {
                while (curr.Value.Left != null &&
                       _comparer.Compare(curr.Value.Key, from) >= 0)
                {
                    centerStack.Push(curr);
                    curr = curr.Value.Left;
                }

                if (_comparer.Compare(curr.Value.Key, to) > 0)
                    yield break;

                if (_comparer.Compare(curr.Value.Key, from) >= 0)
                    yield return curr;

                while (curr.Value.Right == null &&
                       centerStack.Count > 0)
                {
                    curr = centerStack.Pop();
                    if (_comparer.Compare(curr.Value.Key, to) <= 0)
                        yield return curr;
                    else
                        yield break;
                }
                curr = curr.Value.Right;
            }
        }

        private void InsertInternal(TKey key, TValue item)
        {
            Shield.AssertInTransaction();
            Shielded<Node> parent = null;
            var targetLoc = _head.Value;
            int comparison = 0;
            while (targetLoc != null)
            {
                parent = targetLoc;
                if ((comparison = _comparer.Compare(targetLoc.Value.Key, key)) > 0)
                    targetLoc = targetLoc.Value.Left;
                else
                    targetLoc = targetLoc.Value.Right;
            }
            var shN = new Shielded<Node>(new Node()
            {
                //Color = Color.Red, // the default anyway.
                Parent = parent,
                Key = key,
                Value = item
            }, this);
            if (parent != null)
            {
                if (comparison > 0)
                    parent.Modify((ref Node p) => p.Left = shN);
                else
                    parent.Modify((ref Node p) => p.Right = shN);
            }
            else
                _head.Value = shN;
            InsertProcedure(shN);
            _count.Commute((ref int c) => c++);
        }

        #region Wikipedia, insertion

        private Shielded<Node> Grandparent(Shielded<Node> n)
        {
            if (n != null && n.Value.Parent != null)
                return n.Value.Parent.Value.Parent;
            else
                return null;
        }

        private Shielded<Node> Uncle(Shielded<Node> n)
        {
            Shielded<Node> g = Grandparent(n);
            if (g == null)
                return null;
            if (n.Value.Parent == g.Value.Left)
                return g.Value.Right;
            else
                return g.Value.Left;
        }

        private void RotateLeft(Shielded<Node> p)
        {
            Shielded<Node> right = null;
            Shielded<Node> parent = null;
            p.Modify((ref Node pInner) =>
            {
                right = pInner.Right;
                parent = pInner.Parent;
                pInner.Right = right.Value.Left;
                pInner.Parent = right;
            });
            right.Modify((ref Node r) =>
            {
                if (r.Left != null)
                    r.Left.Modify((ref Node n) => n.Parent = p);
                r.Left = p;
                r.Parent = parent;
            });
            if (parent != null)
                parent.Modify((ref Node n) =>
                {
                    if (n.Left == p)
                        n.Left = right;
                    else
                        n.Right = right;
                });
            else
                _head.Value = right;
        }

        private void RotateRight(Shielded<Node> p)
        {
            Shielded<Node> left = null;
            Shielded<Node> parent = null;
            p.Modify((ref Node pInner) =>
            {
                left = pInner.Left;
                parent = pInner.Parent;
                pInner.Left = left.Value.Right;
                pInner.Parent = left;
            });
            left.Modify((ref Node l) =>
            {
                if (l.Right != null)
                    l.Right.Modify((ref Node n) => n.Parent = p);
                l.Right = p;
                l.Parent = parent;
            });
            if (parent != null)
                parent.Modify((ref Node n) =>
                {
                    if (n.Left == p)
                        n.Left = left;
                    else
                        n.Right = left;
                });
            else
                _head.Value = left;
        }

        private void InsertProcedure(Shielded<Node> n)
        {
            while (true)
            {
                if (n.Value.Parent == null)
                    n.Modify((ref Node nInn) => nInn.Color = Color.Black);
                else if (n.Value.Parent.Value.Color == Color.Black)
                    return;
                else
                {
                    Shielded<Node> u = Uncle(n);
                    Shielded<Node> g = Grandparent(n);
                    if (u != null && u.Value.Color == Color.Red)
                    {
                        n.Value.Parent.Modify((ref Node p) => p.Color = Color.Black);
                        u.Modify((ref Node uInn) => uInn.Color = Color.Black);
                        g.Modify((ref Node gInn) => gInn.Color = Color.Red);
                        n = g;
                        continue;
                    }
                    else
                    {
                        if (n == n.Value.Parent.Value.Right && n.Value.Parent == g.Value.Left)
                        {
                            RotateLeft(n.Value.Parent);
                            n = n.Value.Left;
                        }
                        else if (n == n.Value.Parent.Value.Left && n.Value.Parent == g.Value.Right)
                        {
                            RotateRight(n.Value.Parent);
                            n = n.Value.Right;
                        }

                        n.Value.Parent.Modify((ref Node p) => p.Color = Color.Black);
                        g.Modify((ref Node gInn) => gInn.Color = Color.Red);
                        if (n == n.Value.Parent.Value.Left)
                            RotateRight(g);
                        else
                            RotateLeft(g);
                    }
                }
                break;
            }
        }

        #endregion

        /// <summary>
        /// Removes an item and returns it. Useful if you could have multiple items under the same key.
        /// </summary>
        public bool RemoveAndReturn(TKey key, out TValue val)
        {
            var node = RangeInternal(key, key).FirstOrDefault();
            if (node != null)
            {
                val = node.Value.Value;
                RemoveInternal(node);
                return true;
            }
            else
            {
                val = null;
                return false;
            }
        }

        private void RemoveInternal(Shielded<Node> node)
        {
            Shield.AssertInTransaction();
            // find the first follower in the right subtree (arbitrary choice..)
            Shielded<Node> follower;
            if (node.Value.Right == null)
                follower = node;
            else
            {
                follower = node.Value.Right;
                while (follower.Value.Left != null)
                    follower = follower.Value.Left;

                // loosing the node value right now!
                node.Modify((ref Node n) =>
                {
                    var f = follower.Value;
                    n.Key = f.Key;
                    n.Value = f.Value;
                });
            }
            DeleteOneChild(follower);
            _count.Commute((ref int c) => c--);
        }

        #region Wikipedia, removal

        Shielded<Node> Sibling(Shielded<Node> n)
        {
            var parent = n.Value.Parent.Value;
            if (n == parent.Left)
                return parent.Right;
            else
                return parent.Left;
        }

        void ReplaceNode(Shielded<Node> target, Shielded<Node> source)
        {
            var targetParent = target.Value.Parent;
            if (source != null)
                source.Modify((ref Node s) => s.Parent = targetParent);
            if (targetParent == null)
                _head.Value = source;
            else
                targetParent.Modify((ref Node p) =>
                {
                    if (p.Left == target)
                        p.Left = source;
                    else
                        p.Right = source;
                });
            ClearLinks(target);
        }

        void DeleteOneChild(Shielded<Node> node)
        {
            // node has at most one child!
            Shielded<Node> child = node.Value.Right == null ? node.Value.Left : node.Value.Right;
            bool deleted = false;

            if (child != null)
            {
                ReplaceNode(node, child);
                deleted = true;
            }
            if (node.Value.Color == Color.Black)
            {
                if (child != null && child.Value.Color == Color.Red)
                    child.Modify((ref Node c) => c.Color = Color.Black);
                else
                {
                    // since we don't represent null leaves as nodes, we must start
                    // the process with the not yet removed parent in this case.
                    if (!deleted)
                        child = node;

                    while (true)
                    {
                        // delete case 1
                        if (child.Value.Parent != null)
                        {
                            // delete case 2
                            Shielded<Node> s = Sibling(child);
         
                            if (s.Value.Color == Color.Red)
                            {
                                child.Value.Parent.Modify((ref Node p) => p.Color = Color.Red);
                                s.Modify((ref Node sInn) => sInn.Color = Color.Black);
                                if (child == child.Value.Parent.Value.Left)
                                    RotateLeft(child.Value.Parent);
                                else
                                    RotateRight(child.Value.Parent);

                                s = Sibling(child);
                            }

                            // delete case 3
                            if ((child.Value.Parent.Value.Color == Color.Black) &&
                                (s.Value.Color == Color.Black) &&
                                (s.Value.Left == null || s.Value.Left.Value.Color == Color.Black) &&
                                (s.Value.Right == null || s.Value.Right.Value.Color == Color.Black))
                            {
                                s.Modify((ref Node sInn) => sInn.Color = Color.Red);
                                child = child.Value.Parent;
                                continue; // back to 1
                            }
                            else
                            {
                                // delete case 4
                                if ((child.Value.Parent.Value.Color == Color.Red) &&
                                    (s.Value.Color == Color.Black) &&
                                    (s.Value.Left == null || s.Value.Left.Value.Color == Color.Black) &&
                                    (s.Value.Right == null || s.Value.Right.Value.Color == Color.Black))
                                {
                                    s.Modify((ref Node sInn) => sInn.Color = Color.Red);
                                    child.Value.Parent.Modify((ref Node p) => p.Color = Color.Black);
                                }
                                else
                                {
                                    // delete case 5
                                    if (s.Value.Color == Color.Black)
                                    {
                                        if ((child == child.Value.Parent.Value.Left) &&
                                            (s.Value.Right == null || s.Value.Right.Value.Color == Color.Black) &&
                                            (s.Value.Left != null && s.Value.Left.Value.Color == Color.Red))
                                        {
                                            s.Modify((ref Node sInn) =>
                                            {
                                                sInn.Color = Color.Red;
                                                sInn.Left.Modify((ref Node l) => l.Color = Color.Black);
                                            });
                                            RotateRight(s);
                                            s = Sibling(child);
                                        }
                                        else if ((child == child.Value.Parent.Value.Right) &&
                                            (s.Value.Left == null || s.Value.Left.Value.Color == Color.Black) &&
                                            (s.Value.Right != null && s.Value.Right.Value.Color == Color.Red))
                                        {
                                            s.Modify((ref Node sInn) =>
                                            {
                                                sInn.Color = Color.Red;
                                                sInn.Right.Modify((ref Node r) => r.Color = Color.Black);
                                            });
                                            RotateLeft(s);
                                            s = Sibling(child);
                                        }
                                    }

                                    // delete case 6
                                    child.Value.Parent.Modify((ref Node p) =>
                                    {
                                        var c = p.Color;
                                        s.Modify((ref Node sInn) => sInn.Color = c);
                                        p.Color = Color.Black;
                                    });
     
                                    if (child == child.Value.Parent.Value.Left)
                                    {
                                        s.Value.Right.Modify((ref Node r) => r.Color = Color.Black);
                                        RotateLeft(child.Value.Parent);
                                    }
                                    else
                                    {
                                        s.Value.Left.Modify((ref Node l) => l.Color = Color.Black);
                                        RotateRight(child.Value.Parent);
                                    }
                                }
                            }
                        }
                        break;
                    }
                }
            }
            if (!deleted)
            {
                // original node is still in the tree, and it still has no children.
                if (node.Value.Parent != null)
                    node.Value.Parent.Modify((ref Node p) => {
                        if (p.Left == node)
                            p.Left = null;
                        else
                            p.Right = null;
                    });
                else
                    _head.Value = null;
                ClearLinks(node);
            }
        }

        #endregion

        #region ICollection implementation

        void ICollection<KeyValuePair<TKey, TValue>>.Add(KeyValuePair<TKey, TValue> item)
        {
            InsertInternal(item.Key, item.Value);
        }

        /// <summary>
        /// Clear this instance. Efficient, O(1).
        /// </summary>
        public void Clear()
        {
            // ridiculously simple :)
            _head.Value = null;
            _count.Value = 0;
        }

        /// <summary>
        /// Checks both the key and value, which may be useful due to the tree supporting multiple
        /// entries with the same key.
        /// </summary>
        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return Shield.InTransaction(() =>
                RangeInternal(item.Key, item.Key).Any(n => n.Value.Value == item.Value));
        }

        void ICollection<KeyValuePair<TKey, TValue>>.CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            Shield.InTransaction(() => {
                if (_count + arrayIndex > array.Length)
                    throw new IndexOutOfRangeException();
                foreach (var kvp in this)
                    array[arrayIndex++] = kvp;
            });
        }

        /// <summary>
        /// Remove the given key-value pair from the tree. Checks both the key and
        /// value, which may be useful due to the tree supporting multiple entries
        /// with the same key.
        /// </summary>
        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            var target = RangeInternal(item.Key, item.Key).FirstOrDefault(n => n.Value.Value == item.Value);
            if (target == null)
                return false;
            RemoveInternal(target);
            return true;
        }

        /// <summary>
        /// Get the number of items in the tree.
        /// </summary>
        public int Count
        {
            get
            {
                return _count;
            }
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.IsReadOnly
        {
            get
            {
                return false;
            }
        }

        #endregion

        #region IDictionary implementation

        /// <summary>
        /// Add the given key and value to the tree. Same key can be added multiple times
        /// into the tree!
        /// </summary>
        public void Add(TKey key, TValue value)
        {
            InsertInternal(key, value);
        }

        /// <summary>
        /// Check if the key is present in the tree.
        /// </summary>
        public bool ContainsKey(TKey key)
        {
            return FindInternal(key) != null;
        }

        /// <summary>
        /// Remove an item with the specified key. If you want to know what got removed, use
        /// RemoveAndReturn.
        /// </summary>
        public bool Remove(TKey key)
        {
            TValue val;
            return RemoveAndReturn(key, out val);
        }

        /// <summary>
        /// Try to get any one of the values stored under the given key. There may be multiple items
        /// under the same key!
        /// </summary>
        public bool TryGetValue(TKey key, out TValue value)
        {
            bool res = false;
            value = Shield.InTransaction(() => {
                var n = FindInternal(key);
                res = n != null;
                return res ? n.Value.Value : null;
            });
            return res;
        }

        /// <summary>
        /// Gets or sets the item with the specified key.
        /// If there are many with the same key, acts on the first one it finds!
        /// </summary>
        public TValue this[TKey key]
        {
            get
            {
                return Shield.InTransaction(() => {
                    var n = FindInternal(key);
                    if (n == null)
                        throw new KeyNotFoundException();
                    return n.Value.Value;
                });
            }
            set
            {
                Shield.AssertInTransaction();
                // replaces the first occurrence...
                var n = FindInternal(key);
                if (n == null)
                    Add(key, value);
                else if (n.Value.Value != value)
                    n.Modify((ref Node nInner) => nInner.Value = value);
            }
        }

        /// <summary>
        /// Get a collection of the keys in the tree. Works out of transaction.
        /// The result is a copy, it does not get updated with later changes.
        /// Count will be equal to the tree count, i.e. if there are multiple
        /// entries with the same key, that key will be in this collection
        /// multiple times.
        /// </summary>
        public ICollection<TKey> Keys
        {
            get
            {
                return Shield.InTransaction(
                    () => ((IEnumerable<KeyValuePair<TKey, TValue>>)this)
                        .Select(kvp => kvp.Key)
                        .ToList());
            }
        }

        /// <summary>
        /// Get a collection of the values in the tree. Works out of transaction.
        /// The result is a copy, it does not get updated with later changes.
        /// </summary>
        public ICollection<TValue> Values
        {
            get
            {
                return Shield.InTransaction(
                    () => ((IEnumerable<KeyValuePair<TKey, TValue>>)this)
                        .Select(kvp => kvp.Value)
                        .ToList());
            }
        }

        #endregion

    }
}

