using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Shielded;

namespace ShieldedTests
{
    [TestFixture()]
    public class TreeTests
    {
        [Test()]
        public void CopyToTest()
        {
            var tree = new ShieldedTree<int, object>();
            ParallelEnumerable.Range(1, 1000).ForAll(
                i => Shield.InTransaction(() => tree.Add(i, null)));
            Assert.AreEqual(1000, tree.Count);

            var array = new KeyValuePair<int, object>[1100];
            tree.CopyTo(array, 100);
            Assert.AreEqual(1, array[100].Key);
            Assert.AreEqual(2, array[101].Key);
            Assert.AreEqual(999, array[1098].Key);
            Assert.AreEqual(1000, array[1099].Key);
        }
    }
}

