using System;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

namespace Shielded
{
    internal class CommittingSubscription : IDisposable
    {
        private static CommittingSubscription[] _whenCommitingSubs;

        public static bool Any
        {
            get
            {
                return _whenCommitingSubs != null;
            }
        }

        public static void Fire(TransItems items)
        {
            var theList = _whenCommitingSubs;
            if (theList == null)
                return;

            var fields = items.Enlisted
                .GroupBy(i => i.Owner)
                .Select(grp => new TransactionField(grp.Key, grp.Any(i => i.HasChanges)))
                .ToArray();
            for (int i = 0; i < theList.Length; i++)
                theList[i].Act(fields);
        }

        public readonly Action<TransactionField[]> Act;

        public CommittingSubscription(Action<TransactionField[]> act)
        {
            Act = act;

            CommittingSubscription[] oldList, newList;
            var newItem = new[] { this };
            do
            {
                oldList = _whenCommitingSubs;
                newList = oldList == null ? newItem : oldList.Concat(newItem).ToArray();
            } while (Interlocked.CompareExchange(ref _whenCommitingSubs, newList, oldList) != oldList);
        }

        public void Dispose()
        {
            CommittingSubscription[] oldList, newList;
            do
            {
                oldList = _whenCommitingSubs;
                newList = oldList.Where(i => i != this).ToArray();
                if (newList.Length == 0)
                    newList = null;
            } while (Interlocked.CompareExchange(ref _whenCommitingSubs, newList, oldList) != oldList);
        }
    }
}

