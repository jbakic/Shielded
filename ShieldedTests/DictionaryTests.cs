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
                    x1 = dict[2];
                });
                t.Start();
                t.Join();

                Assert.IsNull(x1);
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
        public void DictionaryRace()
        {
            // a race over just one element
            var dict = new ShieldedDict<int, int>();
            int transactionCount = 0;
            Task.WaitAll(
                Enumerable.Repeat(1, 100).Select(i => Task.Factory.StartNew(() =>
                {
                    Shield.InTransaction(() =>
                    {
                        Interlocked.Increment(ref transactionCount);
                        var a = dict[1];
                        Thread.Sleep(5);
                        dict[1] = a + 1;
                    });
                }, TaskCreationOptions.LongRunning)).ToArray());
            Assert.AreEqual(100, dict[1]);
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

