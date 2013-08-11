using System;
using System.Linq;
using System.Linq.Expressions;
using System.Collections;
using System.Collections.Generic;

namespace Shielded
{
    /// <summary>
    /// A shielded red-black tree. Each node is a Shielded struct, so parallel
    /// operations are possible.
    /// </summary>
    public class ShieldedTree<T, TKey> : IEnumerable<T> where T : class
    {
        private enum Color
        {
            Red = 0,
            Black
        }

        private struct Node
        {
            public Color Color;
            public T Value;
            public Shielded<Node> Left;
            public Shielded<Node> Right;
            public Shielded<Node> Parent;
        }

        private readonly ShieldedRef<Shielded<Node>> _head;
        private readonly Func<T, TKey> _keySelector;
        private readonly IComparer<TKey> _comparer;

        public ShieldedTree(Func<T, TKey> keySelector, IComparer<TKey> comparer = null)
        {
            _head = new ShieldedRef<Shielded<Node>>();
            _keySelector = keySelector;
            _comparer = comparer != null ? comparer : Comparer<TKey>.Default;
        }

        public T Find(TKey key)
        {
            var curr = _head.Read;
            int comparison;
            while (curr != null &&
                   (comparison = _comparer.Compare(_keySelector(curr.Read.Value), key)) != 0)
            {
                if (comparison > 0)
                    curr = curr.Read.Left;
                else
                    curr = curr.Read.Right;
            }
            return curr != null ? curr.Read.Value : null;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<T>)this).GetEnumerator();
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
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

                yield return curr.Read.Value;

                while (curr.Read.Right == null && centerStack.Count > 0)
                {
                    curr = centerStack.Pop();
                    yield return curr.Read.Value;
                }
                curr = curr.Read.Right;
            }
        }

        public IEnumerable<T> Range(TKey from, TKey to)
        {
            foreach (var n in RangeInternal(from, to))
                yield return n.Read.Value;
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
                       _comparer.Compare(_keySelector(curr.Read.Value), from) >= 0)
                {
                    centerStack.Push(curr);
                    curr = curr.Read.Left;
                }

                if (_comparer.Compare(_keySelector(curr.Read.Value), from) >= 0 &&
                    _comparer.Compare(_keySelector(curr.Read.Value), to) <= 0)
                    yield return curr;

                while (curr.Read.Right == null &&
                       _comparer.Compare(_keySelector(curr.Read.Value), to) <= 0 &&
                       centerStack.Count > 0)
                {
                    curr = centerStack.Pop();
                    if (_comparer.Compare(_keySelector(curr.Read.Value), from) >= 0 &&
                        _comparer.Compare(_keySelector(curr.Read.Value), to) <= 0)
                        yield return curr;
                }
                curr = curr.Read.Right;
            }
        }

        public void Insert(T item)
        {
            TKey itemKey = _keySelector(item);
            Shield.InTransaction(() =>
            {
                Shielded<Node> parent = null;
                var targetLoc = _head.Read;
                int comparison = 0;
                while (targetLoc != null //&&
                       /*(comparison = _comparer.Compare(_keySelector(targetLoc.Read.Value), itemKey)) != 0*/)
                {
                    parent = targetLoc;
                    if ((comparison = _comparer.Compare(_keySelector(targetLoc.Read.Value), itemKey)) > 0)
                        targetLoc = targetLoc.Read.Left;
                    else
                        targetLoc = targetLoc.Read.Right;
                }
                var shN = new Shielded<Node>(new Node()
                {
                    //Color = Color.Red, // the default anyway.
                    Parent = parent,
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
            Shielded<Node> right = p.Read.Right;
            p.Modify((ref Node pInner) => pInner.Right = right.Read.Left);
            if (right.Read.Left != null)
                right.Read.Left.Modify((ref Node n) => n.Parent = p);
            right.Modify((ref Node r) => r.Left = p);
            if (p.Read.Parent != null)
                if (p.Read.Parent.Read.Left == p)
                    p.Read.Parent.Modify((ref Node n) => n.Left = right);
                else
                    p.Read.Parent.Modify((ref Node n) => n.Right = right);
            else
                _head.Assign(right);
            right.Modify((ref Node r) => r.Parent = p.Read.Parent);
            p.Modify((ref Node pInner) => pInner.Parent = right);
        }

        private void RotateRight(Shielded<Node> p)
        {
            Shielded<Node> left = p.Read.Left;
            p.Modify((ref Node pInner) => pInner.Left = left.Read.Right);
            if (left.Read.Right != null)
                left.Read.Right.Modify((ref Node n) => n.Parent = p);
            left.Modify((ref Node l) => l.Right = p);
            if (p.Read.Parent != null)
                if (p.Read.Parent.Read.Left == p)
                    p.Read.Parent.Modify((ref Node n) => n.Left = left);
                else
                    p.Read.Parent.Modify((ref Node n) => n.Right = left);
            else
                _head.Assign(left);
            left.Modify((ref Node l) => l.Parent = p.Read.Parent);
            p.Modify((ref Node pInner) => pInner.Parent = left);
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

        public void Remove(T item)
        {
            var key = _keySelector(item);
            Shield.InTransaction(() =>
            {
                var node = RangeInternal(key, key).FirstOrDefault(n => n.Read.Value == item);
                if (node != null)
                    RemoveInternal(node);
                else
                    throw new KeyNotFoundException();
            });
        }

        public T Remove(TKey key)
        {
            T retVal = null;
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
                node.Modify((ref Node n) => n.Value = follower.Read.Value);
            }
            DeleteOneChild(follower);
        }

        Shielded<Node> Sibling(Shielded<Node> n)
        {
            if (n == n.Read.Parent.Read.Left)
                return n.Read.Parent.Read.Right;
            else
                return n.Read.Parent.Read.Left;
        }

        void ReplaceNode(Shielded<Node> target, Shielded<Node> source)
        {
            var targetParent = target.Read.Parent;
            if (source != null)
                source.Modify((ref Node s) => s.Parent = targetParent);
            if (targetParent == null)
                _head.Assign(source);
            else if (targetParent.Read.Left == target)
                targetParent.Modify((ref Node p) => p.Left = source);
            else
                targetParent.Modify((ref Node p) => p.Right = source);
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
    }
}

