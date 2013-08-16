using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Shielded;

namespace ShieldedTests
{
    [TestFixture()]
    public class Test
    {
        [Test()]
        public void BasicTest()
        {
            Shielded<int> a = new Shielded<int>(5);
            Assert.AreEqual(5, a);

            try
            {
                a.Modify((ref int n) => n = 10);
                Assert.Fail();
            }
            catch (InvalidOperationException) {}

            Assert.IsFalse(Shield.IsInTransaction);
            Shield.InTransaction(() =>
            {
                a.Modify((ref int n) => n = 20);
                // the TPL sometimes executes tasks on the same thread.
                int x1 = 0;
                var t = new Thread(() =>
                {
                    Assert.IsFalse(Shield.IsInTransaction);
                    x1 = a;
                });
                t.Start();
                t.Join();

                Assert.IsTrue(Shield.IsInTransaction);
                Assert.AreEqual(5, x1);
                Assert.AreEqual(20, a);
            });
            Assert.IsFalse(Shield.IsInTransaction);

            int x2 = 0;
            var t2 = new Thread(() =>
            {
                Assert.IsFalse(Shield.IsInTransaction);
                x2 = a;
            });
            t2.Start();
            t2.Join();
            Assert.AreEqual(20, x2);
            Assert.AreEqual(20, a);
        }

        [Test]
        public void RaceTest()
        {
            var x = new Shielded<int>();
            int transactionCount = 0;
            Task.WaitAll(
                Enumerable.Repeat(1, 100).Select(i => Task.Factory.StartNew(() =>
                {
                    Shield.InTransaction(() =>
                    {
                        Interlocked.Increment(ref transactionCount);
                        int a = x;
                        Thread.Sleep(5);
                        x.Assign(a + i);
                    });
                }, TaskCreationOptions.LongRunning)).ToArray());
            Assert.AreEqual(100, x);
            // just to confirm validity of test! not really a fail if this fails.
            Assert.Greater(transactionCount, 100);
        }

        [Test]
        public void SkewWriteTest()
        {
            var cats = new Shielded<int>(1);
            var dogs = new Shielded<int>(1);
            Task.WaitAll(
                Enumerable.Range(1, 2).Select(i => Task.Factory.StartNew(() =>
                {
                    Shield.InTransaction(() =>
                    {
                        if (cats + dogs < 3)
                        {
                            Thread.Sleep(200);
                            if (i == 1)
                                cats.Modify((ref int n) => n++);
                            else
                                dogs.Modify((ref int n) => n++);
                        }
                    });
                }, TaskCreationOptions.LongRunning)).ToArray());
            Assert.AreEqual(3, cats + dogs);
        }
    }
}

