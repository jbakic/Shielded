using System;
using System.Collections.Generic;
using System.Linq;

namespace Shielded
{
    internal class TransItems
    {
        public SimpleHashSet Enlisted = new SimpleHashSet();
        public bool HasChanges;
        public List<SideEffect> Fx;
        public List<Action> SyncFx;
        public List<Commute> Commutes;

        /// <summary>
        /// Unions the other items into this. Does not include commutes!
        /// </summary>
        public void UnionWith(TransItems other)
        {
            Enlisted.UnionWith(other.Enlisted);
            HasChanges = HasChanges || other.HasChanges;
            ListMerge(ref Fx, other.Fx);
            ListMerge(ref SyncFx, other.SyncFx);
        }

        private static void ListMerge<T>(ref List<T> list, List<T> toAppend)
        {
            if (toAppend == null)
                return;
            if (list == null)
                list = new List<T>(toAppend);
            else
                list.AddRange(toAppend);
        }

        public TransactionField[] GetFields()
        {
            return Enlisted
                .GroupBy(i => i.Owner)
                .Select(grp => new TransactionField(grp.Key, grp.Any(i => i.HasChanges)))
                .ToArray();
        }
    }
}

