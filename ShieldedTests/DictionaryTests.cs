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
    }
}

