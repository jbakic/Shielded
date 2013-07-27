using System;
using System.Linq;
using System.Linq.Expressions;
using System.Collections;
using System.Collections.Generic;

namespace Trans
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

        public void Insert(T item)
        {
            TKey itemKey = _keySelector(item);
            Shield.InTransaction(() =>
            {
                Shielded<Node> parent = null;
                var targetLoc = _head.Read;
                int comparison = 0;
                while (targetLoc != null &&
                       (comparison = _comparer.Compare(_keySelector(targetLoc.Read.Value), itemKey)) != 0)
                {
                    parent = targetLoc;
                    if (comparison > 0)
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
            Remove(_keySelector(item));
        }

        public void Remove(TKey key)
        {

        }
    }
}

