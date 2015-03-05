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
        [Test]
        public void IndexerTest()
        {
            var objectA = new object();
            var objectB = new object();
            var objectC = new object();
            var tree = new ShieldedTree<string, object>();
            Shield.InTransaction(() => {
                tree.Add("key a", objectA);
                tree.Add("key b", objectB);
                tree.Add("key c", objectC);
            });
            Assert.AreEqual(3, tree.Count);
            Assert.AreEqual(objectA, tree["key a"]);
            Assert.AreEqual(objectB, tree["key b"]);
            Assert.AreEqual(objectC, tree["key c"]);

            Assert.Throws<KeyNotFoundException>(() => {
                var x = tree["not me"];
            });
            Shield.InTransaction(() =>
                Assert.Throws<KeyNotFoundException>(() => {
                    var x = tree["not me"];
                }));

            Shield.InTransaction(() => {
                tree["key a"] = objectC;
                Assert.AreEqual(3, tree.Count);
                Assert.AreEqual(objectC, tree["key a"]);
                Assert.AreEqual(objectB, tree["key b"]);
                Assert.AreEqual(objectC, tree["key c"]);
            });
            Assert.AreEqual(3, tree.Count);
            Assert.AreEqual(objectC, tree["key a"]);
            Assert.AreEqual(objectB, tree["key b"]);
            Assert.AreEqual(objectC, tree["key c"]);

            var objectD = new object();
            Shield.InTransaction(() => {
                tree["a new one"] = objectD;
                Assert.AreEqual(4, tree.Count);
                Assert.AreEqual(objectC, tree["key a"]);
                Assert.AreEqual(objectB, tree["key b"]);
                Assert.AreEqual(objectC, tree["key c"]);
                Assert.AreEqual(objectD, tree["a new one"]);
            });
            Assert.AreEqual(4, tree.Count);
            Assert.AreEqual(objectC, tree["key a"]);
            Assert.AreEqual(objectB, tree["key b"]);
            Assert.AreEqual(objectC, tree["key c"]);
            Assert.AreEqual(objectD, tree["a new one"]);
        }

        [Test]
        public void EnumerationTest()
        {
            var ordinaryDict = new Dictionary<int, object>() {
                { 1, new object() },
                { 101, new object() },
                { 666999, new object() }
            };
            var tree = new ShieldedTree<int, object>();
            Shield.InTransaction(() => {
                foreach (var kvp in ordinaryDict)
                    tree.Add(kvp.Key, kvp.Value);
            });

            Shield.InTransaction(() => {
                int lastSeen = -1;
                int count = 0;
                var checkSet = new HashSet<int>();
                foreach (var kvp in tree)
                {
                    Assert.IsTrue(checkSet.Add(kvp.Key));
                    Assert.IsTrue(ordinaryDict.ContainsKey(kvp.Key));
                    Assert.AreEqual(ordinaryDict[kvp.Key], kvp.Value);
                    Assert.Greater(kvp.Key, lastSeen);
                    lastSeen = kvp.Key;
                    count++;
                }
            });
        }

        [Test]
        public void RangeTest()
        {
            ShieldedTree<int, object> tree = null;
            Shield.InTransaction(() => {
                // initializer syntax calls Add, so is allowed only in transaction.
                tree = new ShieldedTree<int, object>() {
                    { 1, null },
                    { 1, null },
                    { 2, null },
                    { 4, null },
                    { 5, null },
                };
            });

            Assert.Throws<InvalidOperationException>(() => {
                foreach (var kvp in tree.Range(1, 5))
                    Assert.Fail();
            });

            Shield.InTransaction(() => {
                Assert.IsFalse(tree.Range(5, 1).Any());
                Assert.IsTrue(tree.Range(1, 1).Select(kvp => kvp.Key).SequenceEqual(new int[] { 1, 1 }));
                Assert.IsTrue(tree.Range(2, 3).Select(kvp => kvp.Key).SequenceEqual(new int[] { 2 }));
                Assert.IsTrue(tree.Range(2, 5).Select(kvp => kvp.Key).SequenceEqual(new int[] { 2, 4, 5 }));
                Assert.IsTrue(tree.Range(2, 100).Select(kvp => kvp.Key).SequenceEqual(new int[] { 2, 4, 5 }));
            });
        }

        [Test]
        public void AddTest()
        {
            var tree = new ShieldedTree<int, object>();

            Assert.Throws<InvalidOperationException>(() =>
                tree.Add(1, new object()));
            Assert.Throws<InvalidOperationException>(() =>
                ((ICollection<KeyValuePair<int, object>>)tree).Add(
                    new KeyValuePair<int, object>(1, new object())));

            var objectA = new object();
            var objectB = new object();
            Shield.InTransaction(() => {
                tree.Add(1, objectA);
                ((ICollection<KeyValuePair<int, object>>)tree).Add(
                    new KeyValuePair<int, object>(2, objectB));
                Assert.AreEqual(2, tree.Count);
                Assert.AreEqual(objectA, tree[1]);
                Assert.AreEqual(objectB, tree[2]);
            });
            Assert.AreEqual(2, tree.Count);
            Assert.AreEqual(objectA, tree[1]);
            Assert.AreEqual(objectB, tree[2]);

            var objectA2 = new object();
            var expectedValues = new HashSet<object>(new object[] { objectA, objectA2 });
            Shield.InTransaction(() => {
                tree.Add(1, objectA2);
                Assert.AreEqual(3, tree.Count);
                Assert.IsTrue(expectedValues.SetEquals(tree.Range(1, 1).Select(kvp => kvp.Value)));
            });
            Assert.AreEqual(3, tree.Count);
            Shield.InTransaction(
                () => Assert.IsTrue(expectedValues.SetEquals(tree.Range(1, 1).Select(kvp => kvp.Value))));
        }

        [Test]
        public void ClearTest()
        {
            ShieldedTree<string, object> tree = null;
            Shield.InTransaction(() => {
                // initializer syntax calls Add, so is allowed only in transaction.
                tree = new ShieldedTree<string, object>() {
                    { "key a", null },
                    { "key b", null },
                    { "key c", null },
                };
            });

            Assert.Throws<InvalidOperationException>(tree.Clear);

            Shield.InTransaction(() => {
                tree.Clear();
                Assert.AreEqual(0, tree.Count);
                foreach (var kvp in tree)
                    Assert.Fail();
            });
            Assert.AreEqual(0, tree.Count);
            Shield.InTransaction(() => {
                foreach (var kvp in tree)
                    Assert.Fail();
            });
        }

        [Test]
        public void ContainsTest()
        {
            ShieldedTree<string, object> tree = null;
            Shield.InTransaction(() => {
                tree = new ShieldedTree<string, object>() {
                    { "key a", null },
                    { "key b", null },
                    { "key c", null },
                };
            });

            Assert.IsTrue(tree.ContainsKey("key a"));
            Assert.IsTrue(((ICollection<KeyValuePair<string, object>>)tree).Contains(
                new KeyValuePair<string, object>("key a", null)));
            Assert.IsFalse(tree.ContainsKey("not me"));
            Assert.IsFalse(((ICollection<KeyValuePair<string, object>>)tree).Contains(
                new KeyValuePair<string, object>("not me", null)));
        }

        [Test]
        public void CopyToTest()
        {
            var tree = new ShieldedTree<int, object>();
            ParallelEnumerable.Range(1, 1000).ForAll(
                i => Shield.InTransaction(() => tree.Add(i, new object())));
            Assert.AreEqual(1000, tree.Count);

            var array = new KeyValuePair<int, object>[1100];
            ((ICollection<KeyValuePair<int, object>>)tree).CopyTo(array, 100);
            var expected = 1;
            foreach (var kvp in array.Skip(100))
            {
                Assert.IsTrue(tree.ContainsKey(kvp.Key));
                Assert.AreEqual(expected, kvp.Key);
                Assert.AreEqual(tree[kvp.Key], kvp.Value);
                expected++;
            }
        }

        [Test]
        public void RemoveTest()
        {
            ShieldedTree<string, object> tree = null;
            Shield.InTransaction(() => {
                tree = new ShieldedTree<string, object>() {
                    { "key a", null },
                    { "key b", null },
                    { "key c", null },
                };
            });

            Assert.Throws<InvalidOperationException>(() =>
                tree.Remove("key a"));
            Assert.Throws<InvalidOperationException>(() =>
                tree.Remove(new KeyValuePair<string, object>("key a", null)));

            Shield.InTransaction(() => {
                tree.Remove("key a");
                Assert.AreEqual(2, tree.Count);
                Assert.IsFalse(tree.ContainsKey("key a"));
            });
            Assert.AreEqual(2, tree.Count);
            Assert.IsFalse(tree.ContainsKey("key a"));

            Shield.InTransaction(() => {
                tree.Remove(new KeyValuePair<string, object>("key b", null));
                Assert.AreEqual(1, tree.Count);
                Assert.IsFalse(tree.ContainsKey("key b"));

                tree.Remove(new KeyValuePair<string, object>("key c", new Object()));
                Assert.AreEqual(1, tree.Count);
                Assert.AreEqual(null, tree["key c"]);
            });
            Assert.AreEqual(1, tree.Count);
            Assert.IsFalse(tree.ContainsKey("key b"));
            // ToList() avoids the need for a transaction, usually needed for enumerating collections.
            Assert.AreEqual("key c", tree.ToList().Single().Key);
        }

        [Test]
        public void TryGetValueTest()
        {
            var objectA = new object();
            ShieldedTree<string, object> tree = null;
            Shield.InTransaction(() => {
                tree = new ShieldedTree<string, object>() {
                    { "key a", objectA },
                };
            });

            object x;
            Assert.IsTrue(tree.TryGetValue("key a", out x));
            Assert.AreEqual(objectA, x);

            object y = null;
            Assert.IsTrue(Shield.InTransaction(() => tree.TryGetValue("key a", out y)));
            Assert.AreEqual(objectA, y);

            Assert.IsFalse(tree.TryGetValue("not me", out y));
            Assert.IsFalse(Shield.InTransaction(() => tree.TryGetValue("not me", out y)));
        }

        [Test]
        public void KeysAndValuesTest()
        {
            var objectA = new object();
            var objectB = new object();
            var objectC = new object();
            ShieldedTree<string, object> tree = null;
            Shield.InTransaction(() => {
                tree = new ShieldedTree<string, object>() {
                    { "key a", objectA },
                    { "key b", objectB },
                    { "key c", objectC },
                };
            });
            var hashKeys = new HashSet<string>(new string[] { "key a", "key b", "key c" });
            var hashValues = new HashSet<object>(new object[] { objectA, objectB, objectC });

            Assert.IsTrue(hashKeys.SetEquals(tree.Keys));
            Assert.IsTrue(hashValues.SetEquals(tree.Values));
        }
    }
}

