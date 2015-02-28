using System;
using System.Collections.Generic;
using System.Threading;

namespace Shielded
{
    internal class CommittingSubscription : IDisposable
    {
        private static List<CommittingSubscription> _whenCommitSubs;

        public static bool Any
        {
            get
            {
                return _whenCommitSubs != null;
            }
        }

        public static void Fire(TransItems items)
        {
            var theList = _whenCommitSubs;
            if (theList == null)
                return;

            for (int i = 0; i < theList.Count; i++)
                theList[i].Act(items);
        }

        public readonly Action<TransItems> Act;

        public CommittingSubscription(Action<TransItems> act)
        {
            Act = act;

            List<CommittingSubscription> oldList, newList;
            do
            {
                oldList = _whenCommitSubs;
                newList = oldList != null ? new List<CommittingSubscription>(oldList) :
                    new List<CommittingSubscription>();
                newList.Add(this);
            } while (Interlocked.CompareExchange(ref _whenCommitSubs, newList, oldList) != oldList);
        }

        public void Dispose()
        {
            List<CommittingSubscription> oldList, newList;
            do
            {
                oldList = _whenCommitSubs;
                if (oldList.Count == 1)
                    newList = null;
                else
                {
                    newList = new List<CommittingSubscription>(oldList);
                    newList.Remove(this);
                }
            } while (Interlocked.CompareExchange(ref _whenCommitSubs, newList, oldList) != oldList);
        }
    }
}

