using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Shielded;

namespace ShieldedTests
{
    [TestFixture()]
    public class DictionaryTests
    {
        [Test()]
        public void BasicTest()
        {
            var dict = new ShieldedDict<int, object>();

            try
            {
                dict[1] = new object();
                Assert.Fail();
            }
            catch (InvalidOperationException) {}

            Shield.InTransaction(() =>
            {
                dict[2] = new object();
                // the TPL sometimes executes tasks on the same thread.
                object x1 = null;
                var t = new Thread(() =>
                {
                    Assert.IsFalse(Shield.IsInTransaction);
                    Assert.IsFalse(dict.ContainsKey(2));
                });
                t.Start();
                t.Join();

                Assert.IsNotNull(dict[2]);
            });

            object x2 = null;
            var t2 = new Thread(() =>
            {
                x2 = dict[2];
            });
            t2.Start();
            t2.Join();
            Assert.IsNotNull(x2);
            Assert.IsNotNull(dict[2]);
        }

        [Test]
        public void SimpleRace()
        {
            var dict = new ShieldedDict<int, int>();
            ParallelEnumerable.Range(0, 100000)
                .ForAll(i => Shield.InTransaction(
                    () => dict[i % 100] = dict.ContainsKey(i % 100) ? dict[i % 100] + 1 : 1));

            Shield.InTransaction(() => {
                for (int i = 0; i < 100; i++)
                    Assert.AreEqual(1000, dict[i]);
            });
        }

        [Test]
        public void DictionaryRace()
        {
            var dict = new ShieldedDict<int, int>(
                Enumerable.Range(0, 100)
                .Select(i => new KeyValuePair<int, int>(i, 0)));
            int transactionCount = 0;

            Task.WaitAll(
                Enumerable.Range(0, 1000).Select(i => Task.Factory.StartNew(() =>
                {
                    Shield.InTransaction(() =>
                    {
                        Interlocked.Increment(ref transactionCount);
                        var a = dict[i % 100];
                        Thread.Sleep(5);
                        dict[i % 100] = a + 1;
                    });
                }, TaskCreationOptions.LongRunning)).ToArray());

            Shield.InTransaction(() => {
                var values = dict.Values;
                foreach (var value in values)
                    Assert.AreEqual(10, value);
            });
            Assert.Greater(transactionCount, 100);
        }

        [Test]
        public void ParallelOps()
        {
            var dict = new ShieldedDict<int, object>();
            int transactionCount = 0;
            Task.WaitAll(
                Enumerable.Range(1, 100).Select(i => Task.Factory.StartNew(() =>
                {
                    Shield.InTransaction(() =>
                    {
                        Interlocked.Increment(ref transactionCount);
                        dict[i] = new object();
                        Thread.Sleep(5);
                    });
                }, TaskCreationOptions.LongRunning)).ToArray());
            Assert.AreEqual(100, transactionCount);
        }

        [Test]
        public void ConstructorAndIndexerTest()
        {
            var objectA = new object();
            var objectB = new object();
            var objectC = new object();
            var dict = new ShieldedDict<string, object>(new KeyValuePair<string, object>[] {
                new KeyValuePair<string, object>("key a", objectA),
                new KeyValuePair<string, object>("key b", objectB),
                new KeyValuePair<string, object>("key c", objectC),
            });
            Assert.AreEqual(3, dict.Count);
            Assert.AreEqual(objectA, dict["key a"]);
            Assert.AreEqual(objectB, dict["key b"]);
            Assert.AreEqual(objectC, dict["key c"]);

            try
            {
                var x = dict["not me"];
            }
            catch (KeyNotFoundException) {}
            Shield.InTransaction(() => {
                try
                {
                    var x = dict["not me"];
                }
                catch (KeyNotFoundException) {}
            });

            Shield.InTransaction(() => {
                dict["key a"] = objectC;
                Assert.AreEqual(3, dict.Count);
                Assert.AreEqual(objectC, dict["key a"]);
                Assert.AreEqual(objectB, dict["key b"]);
                Assert.AreEqual(objectC, dict["key c"]);

                dict["key a"] = objectB;
                Assert.AreEqual(3, dict.Count);
                Assert.AreEqual(objectB, dict["key a"]);
                Assert.AreEqual(objectB, dict["key b"]);
                Assert.AreEqual(objectC, dict["key c"]);
            });
            Assert.AreEqual(3, dict.Count);
            Assert.AreEqual(objectB, dict["key a"]);
            Assert.AreEqual(objectB, dict["key b"]);
            Assert.AreEqual(objectC, dict["key c"]);

            var objectD = new object();
            Shield.InTransaction(() => {
                dict["a new one"] = objectD;
                Assert.AreEqual(4, dict.Count);
                Assert.AreEqual(objectB, dict["key a"]);
                Assert.AreEqual(objectB, dict["key b"]);
                Assert.AreEqual(objectC, dict["key c"]);
                Assert.AreEqual(objectD, dict["a new one"]);
            });
            Assert.AreEqual(4, dict.Count);
            Assert.AreEqual(objectB, dict["key a"]);
            Assert.AreEqual(objectB, dict["key b"]);
            Assert.AreEqual(objectC, dict["key c"]);
            Assert.AreEqual(objectD, dict["a new one"]);
        }

        [Test]
        public void EnumerationTest()
        {
            var ordinaryDict = new Dictionary<int, object>() {
                { 1, new object() },
                { 101, new object() },
                { 666999, new object() }
            };
            var dict = new ShieldedDict<int, object>(ordinaryDict);

            var addedObject = new object();
            // in preparation for the same changes done to dict inside transaction
            ordinaryDict.Add(2, addedObject);
            ordinaryDict.Remove(666999);

            Shield.InTransaction(() => {
                // as an IShielded implementor, the Dict is more complex and needs to be more carefully
                // tested for how well he manages thread-local data. So, we add some changes here.
                dict.Add(2, addedObject);
                dict.Remove(666999);

                int count = 0;
                var checkSet = new HashSet<int>();
                foreach (var kvp in dict)
                {
                    Assert.IsTrue(checkSet.Add(kvp.Key));
                    Assert.IsTrue(ordinaryDict.ContainsKey(kvp.Key));
                    Assert.AreEqual(ordinaryDict[kvp.Key], kvp.Value);
                    count++;
                }
                Assert.AreEqual(3, count);
            });
        }

        [Test]
        public void AddTest()
        {
            var dict = new ShieldedDict<int, object>();

            try
            {
                dict.Add(1, new object());
                Assert.Fail();
            }
            catch (InvalidOperationException) {}
            try
            {
                ((ICollection<KeyValuePair<int, object>>)dict).Add(
                    new KeyValuePair<int, object>(1, new object()));
                Assert.Fail();
            }
            catch (InvalidOperationException) {}

            var objectA = new object();
            var objectB = new object();
            Shield.InTransaction(() => {
                dict.Add(1, objectA);
                ((ICollection<KeyValuePair<int, object>>)dict).Add(
                    new KeyValuePair<int, object>(2, objectB));
                Assert.AreEqual(2, dict.Count);
                Assert.AreEqual(objectA, dict[1]);
                Assert.AreEqual(objectB, dict[2]);
            });
            Assert.AreEqual(2, dict.Count);
            Assert.AreEqual(objectA, dict[1]);
            Assert.AreEqual(objectB, dict[2]);
        }

        [Test]
        public void ClearTest()
        {
            var dict = new ShieldedDict<string, object>(new KeyValuePair<string, object>[] {
                new KeyValuePair<string, object>("key a", null),
                new KeyValuePair<string, object>("key b", null),
                new KeyValuePair<string, object>("key c", null),
            });

            try
            {
                dict.Clear();
                Assert.Fail();
            }
            catch (InvalidOperationException) {}

            Shield.InTransaction(() => {
                dict.Clear();
                Assert.AreEqual(0, dict.Count);
                foreach (var kvp in dict)
                    Assert.Fail();
            });
            Assert.AreEqual(0, dict.Count);
            Shield.InTransaction(() => {
                foreach (var kvp in dict)
                    Assert.Fail();
            });
        }

        [Test]
        public void ContainsTest()
        {
            var dict = new ShieldedDict<string, object>(new KeyValuePair<string, object>[] {
                new KeyValuePair<string, object>("key a", null),
                new KeyValuePair<string, object>("key b", null),
                new KeyValuePair<string, object>("key c", null),
            });

            Assert.IsTrue(dict.ContainsKey("key a"));
            Assert.IsTrue(((ICollection<KeyValuePair<string, object>>)dict).Contains(
                new KeyValuePair<string, object>("key a", null)));
            Assert.IsFalse(dict.ContainsKey("not me"));
            Assert.IsFalse(((ICollection<KeyValuePair<string, object>>)dict).Contains(
                new KeyValuePair<string, object>("not me", null)));

            Shield.InTransaction(() => {
                dict.Add("new key", null);
                dict.Remove("key a");
                Assert.IsFalse(dict.ContainsKey("key a"));
                Assert.IsFalse(((ICollection<KeyValuePair<string, object>>)dict).Contains(
                    new KeyValuePair<string, object>("key a", null)));
                Assert.IsTrue(dict.ContainsKey("new key"));
                Assert.IsTrue(((ICollection<KeyValuePair<string, object>>)dict).Contains(
                    new KeyValuePair<string, object>("new key", null)));
            });
        }

        [Test]
        public void CopyToTest()
        {
            var dict = new ShieldedDict<int, object>(
                Enumerable.Range(1, 1000).Select(i =>
                    new KeyValuePair<int, object>(i, new object())));
            Assert.AreEqual(1000, dict.Count);

            var array = new KeyValuePair<int, object>[1100];
            // this works out of transaction (and consequently, so do ToArray and ToList), by opening
            // a transaction itself. that could not be done when you do a foreach.
            ((ICollection<KeyValuePair<int, object>>)dict).CopyTo(array, 100);
            var keys = new HashSet<int>();
            foreach (var kvp in array.Skip(100))
            {
                Assert.IsTrue(dict.ContainsKey(kvp.Key));
                Assert.IsTrue(keys.Add(kvp.Key));
                Assert.AreEqual(dict[kvp.Key], kvp.Value);
            }
        }

        [Test]
        public void RemoveTest()
        {
            var dict = new ShieldedDict<string, object>(new KeyValuePair<string, object>[] {
                new KeyValuePair<string, object>("key a", null),
                new KeyValuePair<string, object>("key b", null),
                new KeyValuePair<string, object>("key c", null),
            });

            try
            {
                dict.Remove("key a");
                Assert.Fail();
            }
            catch (InvalidOperationException) {}
            try
            {
                ((ICollection<KeyValuePair<string, object>>)dict).Remove(
                    new KeyValuePair<string, object>("key a", null));
                Assert.Fail();
            }
            catch (InvalidOperationException) {}

            Shield.InTransaction(() => {
                dict.Remove("key a");
                Assert.AreEqual(2, dict.Count);
                Assert.IsFalse(dict.ContainsKey("key a"));
            });
            Assert.AreEqual(2, dict.Count);
            Assert.IsFalse(dict.ContainsKey("key a"));

            Shield.InTransaction(() => {
                ((ICollection<KeyValuePair<string, object>>)dict).Remove(
                    new KeyValuePair<string, object>("key b", null));
                Assert.AreEqual(1, dict.Count);
                Assert.IsFalse(dict.ContainsKey("key b"));
            });
            Assert.AreEqual(1, dict.Count);
            Assert.IsFalse(dict.ContainsKey("key b"));
            // ToList() avoids the need for a transaction, usually needed for enumerating collections.
            Assert.AreEqual("key c", dict.ToList().Single().Key);
        }

        [Test]
        public void TryGetValueTest()
        {
            var objectA = new object();
            var dict = new ShieldedDict<string, object>(new KeyValuePair<string, object>[] {
                new KeyValuePair<string, object>("key a", objectA),
            });

            object x;
            Assert.IsTrue(dict.TryGetValue("key a", out x));
            Assert.AreEqual(objectA, x);

            object y = null;
            Assert.IsTrue(Shield.InTransaction(() => dict.TryGetValue("key a", out y)));
            Assert.AreEqual(objectA, y);

            Assert.IsFalse(dict.TryGetValue("not me", out y));
            Assert.IsFalse(Shield.InTransaction(() => dict.TryGetValue("not me", out y)));
        }

        [Test]
        public void KeysAndValuesTest()
        {
            var objectA = new object();
            var objectB = new object();
            var objectC = new object();
            var dict = new ShieldedDict<string, object>(new KeyValuePair<string, object>[] {
                new KeyValuePair<string, object>("key a", objectA),
                new KeyValuePair<string, object>("key b", objectB),
                new KeyValuePair<string, object>("key c", objectC),
            });
            var hashKeys = new HashSet<string>(new string[] { "key a", "key b", "key c" });
            var hashValues = new HashSet<object>(new object[] { objectA, objectB, objectC });

            Assert.IsTrue(hashKeys.SetEquals(dict.Keys));
            Assert.IsTrue(hashValues.SetEquals(dict.Values));
        }
    }
}

