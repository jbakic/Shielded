using System;
using System.Threading;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Shielded
{
    internal abstract class CommitLock
    {
    }

    /// <summary>
    /// This class does locking for the commit checks. If two threads use this, they
    /// conflict only if their transaction items overlap.
    /// </summary>
    internal static class CommitLocker
    {
        private class LockState
        {
            public readonly bool Deleted;
            public readonly LockNode Next;

            public LockState(bool deleted, LockNode next)
            {
                Deleted = deleted;
                Next = next;
            }
        }

        private class LockNode : CommitLock
        {
            public LockState State;
            public ISet<IShielded> Enlisted;
            public ISet<IShielded> CommEnlisted;
        }

        private static LockNode _head;

        private static bool IsConflict(LockNode a, LockNode b)
        {
            return
                a.Enlisted.Overlaps(b.Enlisted) ||
                b.CommEnlisted != null && a.Enlisted.Overlaps(b.CommEnlisted) ||
                (a.CommEnlisted != null &&
                    (a.CommEnlisted.Overlaps(b.Enlisted) ||
                    (b.CommEnlisted != null && a.CommEnlisted.Overlaps(b.CommEnlisted))));
        }

        public static void Enter(ISet<IShielded> enlisted, ISet<IShielded> commEnlisted, out CommitLock l)
        {
            var newNode = new LockNode() {
                Enlisted = enlisted,
                CommEnlisted = commEnlisted
            };
            LockNode lastRoundChecked = null;
            l = null;
            do
            {
                newNode.State = new LockState(false, _head);
                var current = newNode.State.Next;
                while (current != null && current != lastRoundChecked)
                {
                    if (!current.State.Deleted && IsConflict(current, newNode))
                        SpinWait.SpinUntil(() => current.State.Deleted);
                    current = current.State.Next;
                }
                lastRoundChecked = newNode.State.Next;

                RuntimeHelpers.PrepareConstrainedRegions();
                try {} finally
                {
                    if (Interlocked.CompareExchange(ref _head, newNode, lastRoundChecked) == lastRoundChecked)
                        l = newNode;
                }
            } while (l == null);
        }

        static void MarkDeleted(LockNode node)
        {
            LockState oldState;
            var newOldState = node.State;
            do
            {
                oldState = newOldState;
                newOldState = Interlocked.CompareExchange(
                    ref node.State, new LockState(true, oldState.Next), oldState);
            } while (oldState != newOldState);
        }

        /// <summary>
        /// Releases the given lock. This method expects to be executed from a finally clause,
        /// since a <see cref="ThreadAbortException"/> might leave the lock system in an invalid
        /// state.
        /// </summary>
        public static void Exit(CommitLock l)
        {
            var node = (LockNode)l;
            MarkDeleted(node);

            // next up, remove us from the list
            while (true)
            {
                var previous = _head;
                if (node == previous &&
                    (previous = Interlocked.CompareExchange(ref _head, node.State.Next, node)) == node)
                    break;
                // if not, proceed until we find our node's previous
                LockState previousState;
                while ((previousState = previous.State).Next != node)
                    previous = previousState.Next;
                if (!previousState.Deleted &&
                    Interlocked.CompareExchange(
                        ref previous.State, new LockState(false, node.State.Next), previousState) == previousState)
                    break;
            }
        }
    }
}

