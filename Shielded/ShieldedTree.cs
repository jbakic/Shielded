using System;
using System.Linq;
using System.Linq.Expressions;
using System.Collections;
using System.Collections.Generic;

namespace Shielded
{
    /// <summary>
    /// A shielded red-black tree. Each node is a Shielded struct, so parallel
    /// operations are possible. Multiple items may be added with the same key.
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

        private readonly Shielded<Shielded<Node>> _head;
        private readonly IComparer<TKey> _comparer;

        public ShieldedTree(IComparer<TKey> comparer = null)
        {
            _head = new Shielded<Shielded<Node>>();
            _comparer = comparer != null ? comparer : Comparer<TKey>.Default;
        }

        private Shielded<Node> FindInternal(TKey key)
        {
            var curr = _head.Read;
            int comparison;
            while (curr != null &&
                   (comparison = _comparer.Compare(curr.Read.Key, key)) != 0)
            {
                if (comparison > 0)
                    curr = curr.Read.Left;
                else
                    curr = curr.Read.Right;
            }
            return curr;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<KeyValuePair<TKey, TValue>>)this).GetEnumerator();
        }

        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
        {
            var a = Shield.CurrentTransactionStartStamp;
            Stack<Shielded<Node>> centerStack = new Stack<Shielded<Node>>();
            var curr = _head.Read;
            while (curr != null)
            {
                while (curr.Read.Left != null)
                {
                    centerStack.Push(curr);
                    curr = curr.Read.Left;
                }

                yield return new KeyValuePair<TKey, TValue>(curr.Read.Key, curr.Read.Value);

                while (curr.Read.Right == null && centerStack.Count > 0)
                {
                    curr = centerStack.Pop();
                    yield return new KeyValuePair<TKey, TValue>(curr.Read.Key, curr.Read.Value);
                }
                curr = curr.Read.Right;
            }
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> Range(TKey from, TKey to)
        {
            foreach (var n in RangeInternal(from, to))
                yield return new KeyValuePair<TKey, TValue>(n.Read.Key, n.Read.Value);
        }

        /// <summary>
        /// Enumerates only over the nodes in the range. borders included.
        /// </summary>
        private IEnumerable<Shielded<Node>> RangeInternal(TKey from, TKey to)
        {
            if (_comparer.Compare(from, to) > 0)
                yield break;
            var a = Shield.CurrentTransactionStartStamp;
            Stack<Shielded<Node>> centerStack = new Stack<Shielded<Node>>();
            var curr = _head.Read;
            while (curr != null)
            {
                while (curr.Read.Left != null &&
                       _comparer.Compare(curr.Read.Key, from) >= 0)
                {
                    centerStack.Push(curr);
                    curr = curr.Read.Left;
                }

                if (_comparer.Compare(curr.Read.Key, from) >= 0 &&
                    _comparer.Compare(curr.Read.Key, to) <= 0)
                    yield return curr;

                while (curr.Read.Right == null &&
                       _comparer.Compare(curr.Read.Key, to) <= 0 &&
                       centerStack.Count > 0)
                {
                    curr = centerStack.Pop();
                    if (_comparer.Compare(curr.Read.Key, from) >= 0 &&
                        _comparer.Compare(curr.Read.Key, to) <= 0)
                        yield return curr;
                }
                if (_comparer.Compare(curr.Read.Key, to) <= 0)
                    curr = curr.Read.Right;
                else
                    break;
            }
        }

        private void InsertInternal(TKey key, TValue item)
        {
            Shield.InTransaction(() =>
            {
                Shielded<Node> parent = null;
                var targetLoc = _head.Read;
                int comparison = 0;
                while (targetLoc != null)
                {
                    parent = targetLoc;
                    if ((comparison = _comparer.Compare(targetLoc.Read.Key, key)) > 0)
                        targetLoc = targetLoc.Read.Left;
                    else
                        targetLoc = targetLoc.Read.Right;
                }
                var shN = new Shielded<Node>(new Node()
                {
                    //Color = Color.Red, // the default anyway.
                    Parent = parent,
                    Key = key,
                    Value = item
                });
                if (parent != null)
                {
                    if (comparison > 0)
                        parent.Modify((ref Node p) => p.Left = shN);
                    else
                        parent.Modify((ref Node p) => p.Right = shN);
                }
                else
                    _head.Assign(shN);
                InsertProcedure(shN);
            });
        }

        #region Wikipedia, insertion

        private Shielded<Node> Grandparent(Shielded<Node> n)
        {
            if (n != null && n.Read.Parent != null)
                return n.Read.Parent.Read.Parent;
            else
                return null;
        }

        private Shielded<Node> Uncle(Shielded<Node> n)
        {
            Shielded<Node> g = Grandparent(n);
            if (g == null)
                return null;
            if (n.Read.Parent == g.Read.Left)
                return g.Read.Right;
            else
                return g.Read.Left;
        }

        private void RotateLeft(Shielded<Node> p)
        {
            Shielded<Node> right = null;
            Shielded<Node> parent = null;
            p.Modify((ref Node pInner) =>
            {
                right = pInner.Right;
                parent = pInner.Parent;
                pInner.Right = right.Read.Left;
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
                _head.Assign(right);
        }

        private void RotateRight(Shielded<Node> p)
        {
            Shielded<Node> left = null;
            Shielded<Node> parent = null;
            p.Modify((ref Node pInner) =>
            {
                left = pInner.Left;
                parent = pInner.Parent;
                pInner.Left = left.Read.Right;
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
                _head.Assign(left);
        }

        private void InsertProcedure(Shielded<Node> n)
        {
            while (true)
            {
                if (n.Read.Parent == null)
                    n.Modify((ref Node nInn) => nInn.Color = Color.Black);
                else if (n.Read.Parent.Read.Color == Color.Black)
                    return;
                else
                {
                    Shielded<Node> u = Uncle(n);
                    Shielded<Node> g = Grandparent(n);
                    if (u != null && u.Read.Color == Color.Red)
                    {
                        n.Read.Parent.Modify((ref Node p) => p.Color = Color.Black);
                        u.Modify((ref Node uInn) => uInn.Color = Color.Black);
                        g.Modify((ref Node gInn) => gInn.Color = Color.Red);
                        n = g;
                        continue;
                    }
                    else
                    {
                        if (n == n.Read.Parent.Read.Right && n.Read.Parent == g.Read.Left)
                        {
                            RotateLeft(n.Read.Parent);
                            n = n.Read.Left;
                        }
                        else if (n == n.Read.Parent.Read.Left && n.Read.Parent == g.Read.Right)
                        {
                            RotateRight(n.Read.Parent);
                            n = n.Read.Right;
                        }

                        n.Read.Parent.Modify((ref Node p) => p.Color = Color.Black);
                        g.Modify((ref Node gInn) => gInn.Color = Color.Red);
                        if (n == n.Read.Parent.Read.Left)
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
        public TValue RemoveAndReturn(TKey key)
        {
            TValue retVal = null;
            Shield.InTransaction(() =>
            {
                retVal = null;
                var node = RangeInternal(key, key).FirstOrDefault();
                if (node != null)
                {
                    retVal = node.Read.Value;
                    RemoveInternal(node);
                }
                else
                    retVal = null;
            });
            return retVal;
        }

        private void RemoveInternal(Shielded<Node> node)
        {
            // find the first follower in the right subtree (arbitrary choice..)
            Shielded<Node> follower;
            if (node.Read.Right == null)
                follower = node;
            else
            {
                follower = node.Read.Right;
                while (follower.Read.Left != null)
                    follower = follower.Read.Left;

                // loosing the node value right now!
                node.Modify((ref Node n) =>
                {
                    var f = follower.Read;
                    n.Key = f.Key;
                    n.Value = f.Value;
                });
            }
            DeleteOneChild(follower);
        }

        #region Wikipedia, removal

        Shielded<Node> Sibling(Shielded<Node> n)
        {
            var parent = n.Read.Parent.Read;
            if (n == parent.Left)
                return parent.Right;
            else
                return parent.Left;
        }

        void ReplaceNode(Shielded<Node> target, Shielded<Node> source)
        {
            var targetParent = target.Read.Parent;
            if (source != null)
                source.Modify((ref Node s) => s.Parent = targetParent);
            if (targetParent == null)
                _head.Assign(source);
            else
                targetParent.Modify((ref Node p) =>
                {
                    if (p.Left == target)
                        p.Left = source;
                    else
                        p.Right = source;
                });
        }

        void DeleteOneChild(Shielded<Node> node)
        {
            // node has at most one child!
            Shielded<Node> child = node.Read.Right == null ? node.Read.Left : node.Read.Right;

            ReplaceNode(node, child);
            if (node.Read.Color == Color.Black)
            {
                if (child != null && child.Read.Color == Color.Red)
                    child.Modify((ref Node c) => c.Color = Color.Black);
                else while (true)
                {
                    // can happen only once.
                    if (child == null) child = node.Read.Parent;

                    // delete case 1
                    if (child.Read.Parent != null)
                    {
                        // delete case 2
                        Shielded<Node> s = Sibling(child);
     
                        if (s.Read.Color == Color.Red)
                        {
                            child.Read.Parent.Modify((ref Node p) => p.Color = Color.Red);
                            s.Modify((ref Node sInn) => sInn.Color = Color.Black);
                            if (child == child.Read.Parent.Read.Left)
                                RotateLeft(child.Read.Parent);
                            else
                                RotateRight(child.Read.Parent);

                            s = Sibling(child);
                        }

                        // delete case 3
                        if ((child.Read.Parent.Read.Color == Color.Black) &&
                            (s != null) && (s.Read.Color == Color.Black) &&
                            (s.Read.Left == null || s.Read.Left.Read.Color == Color.Black) &&
                            (s.Read.Right == null || s.Read.Right.Read.Color == Color.Black))
                        {
                            s.Modify((ref Node sInn) => sInn.Color = Color.Red);
                            child = child.Read.Parent;
                            continue; // back to 1
                        }
                        else
                        {
                            // delete case 4
                            if ((child.Read.Parent.Read.Color == Color.Red) &&
                                (s.Read.Color == Color.Black) &&
                                (s.Read.Left == null || s.Read.Left.Read.Color == Color.Black) &&
                                (s.Read.Right == null || s.Read.Right.Read.Color == Color.Black))
                            {
                                s.Modify((ref Node sInn) => sInn.Color = Color.Red);
                                child.Read.Parent.Modify((ref Node p) => p.Color = Color.Black);
                            }
                            else
                            {
                                // delete case 5
                                if (s.Read.Color == Color.Black)
                                {
                                    if ((child == child.Read.Parent.Read.Left) &&
                                        (s.Read.Right == null || s.Read.Right.Read.Color == Color.Black) &&
                                        (s.Read.Left != null && s.Read.Left.Read.Color == Color.Red))
                                    {
                                        s.Modify((ref Node sInn) =>
                                        {
                                            sInn.Color = Color.Red;
                                            sInn.Left.Modify((ref Node l) => l.Color = Color.Black);
                                        });
                                        RotateRight(s);
                                        s = Sibling(child);
                                    }
                                    else if ((child == child.Read.Parent.Read.Right) &&
                                        (s.Read.Left == null || s.Read.Left.Read.Color == Color.Black) &&
                                        (s.Read.Right != null && s.Read.Right.Read.Color == Color.Red))
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
                                child.Read.Parent.Modify((ref Node p) =>
                                {
                                    var c = p.Color;
                                    s.Modify((ref Node sInn) => sInn.Color = c);
                                    p.Color = Color.Black;
                                });
 
                                if (child == child.Read.Parent.Read.Left)
                                {
                                    s.Read.Right.Modify((ref Node r) => r.Color = Color.Black);
                                    RotateLeft(child.Read.Parent);
                                }
                                else
                                {
                                    s.Read.Left.Modify((ref Node l) => l.Color = Color.Black);
                                    RotateRight(child.Read.Parent);
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }

        #endregion

        #region ICollection implementation

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            InsertInternal(item.Key, item.Value);
        }

        public void Clear()
        {
            // ridiculously simple :)
            _head.Assign(null);
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return RangeInternal(item.Key, item.Key).Any(n => n.Read.Value == item.Value);
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            throw new System.NotImplementedException();
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            var target = RangeInternal(item.Key, item.Key).FirstOrDefault(n => n.Read.Value == item.Value);
            if (target == null)
                return false;
            RemoveInternal(target);
            return true;
        }

        /// <summary>
        /// Currently iterates over the whole tree to get the count! Do not use.
        /// </summary>
        public int Count
        {
            get
            {
                return ((IEnumerable<KeyValuePair<TKey, TValue>>)this).Count();
            }
        }

        public bool IsReadOnly
        {
            get
            {
                return false;
            }
        }

        #endregion

        #region IDictionary implementation

        public void Add(TKey key, TValue value)
        {
            InsertInternal(key, value);
        }

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
            return RemoveAndReturn(key) != null;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            var n = FindInternal(key);
            if (n == null)
            {
                value = null;
                return false;
            }
            else
            {
                value = n.Read.Value;
                return true;
            }
        }

        /// <summary>
        /// Gets or sets the item with the specified key.
        /// If there are many with the same key, acts on the first one it finds!
        /// </summary>
        public TValue this[TKey key]
        {
            get
            {
                var n = FindInternal(key);
                if (n == null)
                    throw new KeyNotFoundException();
                return n.Read.Value;
            }
            set
            {
                // replaces the first occurrence...
                var n = FindInternal(key);
                if (n == null)
                    throw new KeyNotFoundException();
                if (n.Read.Value != value)
                    n.Modify((ref Node nInner) => nInner.Value = value);
            }
        }

        public ICollection<TKey> Keys
        {
            get
            {
                return ((IEnumerable<KeyValuePair<TKey, TValue>>)this)
                    .Select(kvp => kvp.Key)
                    .ToList();
            }
        }

        public ICollection<TValue> Values
        {
            get
            {
                return ((IEnumerable<KeyValuePair<TKey, TValue>>)this)
                    .Select(kvp => kvp.Value)
                    .ToList();
            }
        }

        #endregion

    }
}

