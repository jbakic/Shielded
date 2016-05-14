using System;
using System.Collections.Generic;
using System.Linq;

namespace Shielded
{
    internal class TransItems
    {
#if USE_STD_HASHSET
        public HashSet<IShielded> Enlisted = new HashSet<IShielded>();
#else
        public SimpleHashSet Enlisted = new SimpleHashSet();
#endif
        public bool HasChanges;
        public List<SideEffect> Fx;
        public List<Commute> Commutes;

        /// <summary>
        /// Unions the other items into this. Does not include commutes!
        /// </summary>
        public void UnionWith(TransItems other)
        {
            Enlisted.UnionWith(other.Enlisted);
            HasChanges = HasChanges || other.HasChanges;
            if (other.Fx != null && other.Fx.Count > 0)
                if (Fx == null)
                    Fx = new List<SideEffect>(other.Fx);
                else
                    Fx.AddRange(other.Fx);
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

