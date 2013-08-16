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
    public class BasicTests
    {
        [Test()]
        public void TransactionSafetyTest()
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

        [Test]
        public void SideEffectTest()
        {
            var x = new Shielded<DateTime>(DateTime.UtcNow);
            // this transaction commits suicide by running a parallel one to conflict with.
            Shield.InTransaction(() =>
            {
                Shield.SideEffect(() => {
                    Assert.Fail("Suicide transaction has committed.");
                },
                () => {
                    // this perhaps should not work :D but it does. prevents repetition.
                    Shield.Rollback(false);
                });
                // this is in case Assign() becomes commutative. then it would never conflict if not read.
                var temp = x.Read;
                x.Assign(DateTime.UtcNow);
                var t = new Thread(() => {
                    Shield.InTransaction(() => {
                        var temp2 = x.Read;
                        x.Assign(DateTime.UtcNow);
                    });
                });
                t.Start();
                t.Join();
            });

            bool commitFx = false;
            Shield.InTransaction(() => {
                Shield.SideEffect(() => {
                    Assert.IsFalse(commitFx);
                    commitFx = true;
                });
            });
            Assert.IsTrue(commitFx);
        }

        [Test]
        public void ConditionalTest()
        {
            var x = new Shielded<int>();
            var testCounter = 0;
            var triggerCommits = 0;

            Shield.Conditional(() => {
                Interlocked.Increment(ref testCounter);
                return x > 0 && (x & 2) == 0;
            },
            () => {
                Shield.SideEffect(() =>
                    Interlocked.Increment(ref triggerCommits));
                Assert.IsTrue(x > 0 && (x & 2) == 0);
                return true;
            });

            const int count = 1000;
            ParallelEnumerable.Repeat(1, count).ForAll(i =>
                Shield.InTransaction(() => x.Modify((ref int n) => n++)));

            // one more, for the first call to Conditional()! btw, if this conditional were to
            // write anywhere, he might conflict, and an interlocked counter would give more due to
            // repetitions. so, this confirms reader progress too.
            Assert.AreEqual(count + 1, testCounter);
            // every change triggers it, but by the time it starts, another transaction might have
            // committed, so this is not a fixed number.
            Assert.Greater(triggerCommits, 0);
        }
    }
}

