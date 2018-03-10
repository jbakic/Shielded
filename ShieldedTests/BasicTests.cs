using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Shielded;

namespace ShieldedTests
{
    [TestFixture]
    public class BasicTests
    {
        [Test]
        public void TransactionSafetyTest()
        {
            var a = new Shielded<int>(5);
            Assert.AreEqual(5, a);

            Assert.Throws<InvalidOperationException>(() =>
                a.Modify((ref int n) => n = 10));

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
                Enumerable.Range(1, 100).Select(i => Task.Factory.StartNew(() =>
                {
                    bool committed = false;
                    try
                    {
                        Shield.InTransaction(() =>
                        {
                            Shield.SideEffect(() => { committed = true; });
                            Interlocked.Increment(ref transactionCount);
                            int a = x;
                            Thread.Sleep(5);
                            x.Value = a + i;
                            if (i == 100)
                                throw new InvalidOperationException();
                        });
                        Assert.AreNotEqual(100, i);
                        Assert.IsTrue(committed);
                    }
                    catch
                    {
                        Assert.AreEqual(100, i);
                        Assert.IsFalse(committed);
                    }
                }, TaskCreationOptions.LongRunning)).ToArray());

            Assert.AreEqual(4950, x);
            if (transactionCount == 100)
                Assert.Inconclusive();
        }

        [Test]
        public void SkewWriteTest()
        {
            var cats = new Shielded<int>(1);
            var dogs = new Shielded<int>(1);
            int transactionCount = 0;
            Task.WaitAll(
                Enumerable.Range(1, 2).Select(i => Task.Factory.StartNew(() =>
                    Shield.InTransaction(() =>
                    {
                        Interlocked.Increment(ref transactionCount);
                        if (cats + dogs < 3)
                        {
                            Thread.Sleep(200);
                            if (i == 1)
                                cats.Modify((ref int n) => n++);
                            else
                                dogs.Modify((ref int n) => n++);
                        }
                    }), TaskCreationOptions.LongRunning)).ToArray());
            Assert.AreEqual(3, cats + dogs);
            Assert.AreEqual(3, transactionCount);
        }

        class IgnoreMe : Exception {}

        [Test]
        public void SideEffectTest()
        {
            var x = new Shielded<DateTime>(DateTime.UtcNow);
            try
            {
                Shield.InTransaction(() =>
                {
                    Shield.SideEffect(() => {
                        Assert.Fail("Suicide transaction has committed.");
                    },
                    () => {
                        throw new IgnoreMe();
                    });
                    // in case the Value setter becomes commutative, we use Modify to ensure conflict.
                    x.Modify((ref DateTime d) => d = DateTime.UtcNow);
                    var t = new Thread(() =>
                        Shield.InTransaction(() =>
                            x.Modify((ref DateTime d) => d = DateTime.UtcNow)));
                    t.Start();
                    t.Join();
                });
                Assert.Fail("Suicide transaction did not throw.");
            }
            catch (AggregateException aggr)
            {
                Assert.AreEqual(1, aggr.InnerExceptions.Count);
                Assert.AreEqual(typeof(IgnoreMe), aggr.InnerException.GetType());
            }

            bool commitFx = false;
            Shield.InTransaction(() => {
                Shield.SideEffect(() => {
                    Assert.IsFalse(commitFx);
                    commitFx = true;
                });
            });
            Assert.IsTrue(commitFx);

            bool outOfTransFx = false, outOfTransOnRollback = false;
            Shield.SideEffect(() => outOfTransFx = true, () => outOfTransOnRollback = true);
            Assert.IsTrue(outOfTransFx);
            Assert.IsFalse(outOfTransOnRollback);
        }

        [Test]
        public void ConditionalTest()
        {
            var x = new Shielded<int>();
            var testCounter = 0;
            var triggerCommits = 0;

            Shield.Conditional(() => {
                Interlocked.Increment(ref testCounter);
                return x > 0 && (x & 1) == 0;
            },
            () => {
                Shield.SideEffect(() =>
                    Interlocked.Increment(ref triggerCommits));
                Assert.IsTrue(x > 0 && (x & 1) == 0);
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


            // a conditional which does not depend on any Shielded is not allowed!
            int a = 5;
            Assert.Throws<InvalidOperationException>(() =>
                Shield.Conditional(() => a > 10, () => { }));

            bool firstTime = true;
            var x2 = new Shielded<int>();
            // this one succeeds in registering, but fails as soon as it gets triggered, due to changing it's
            // test's access pattern to an empty set.
            Shield.Conditional(() => {
                if (firstTime)
                {
                    firstTime = false;
                    return x2 == 0;
                }
                else
                    // this is of course invalid, and when reaching here we have not touched any Shielded obj.
                    return true;
            }, () => { });

            try
            {
                // this will trigger the conditional
                Shield.InTransaction(() => x2.Modify((ref int n) => n++));
                Assert.Fail();
            }
            catch (AggregateException aggr)
            {
                Assert.AreEqual(1, aggr.InnerExceptions.Count);
                Assert.AreEqual(typeof(InvalidOperationException), aggr.InnerException.GetType());
            }
        }

        [Test]
        public void EventTest()
        {
            var a = new Shielded<int>(1);
            var eventCount = new Shielded<int>();
            EventHandler<EventArgs> ev =
                (sender, arg) => eventCount.Commute((ref int e) => e++);

            Assert.Throws<InvalidOperationException>(() =>
                a.Changed.Subscribe(ev));

            Shield.InTransaction(() =>
            {
                a.Changed.Subscribe(ev);

                var t = new Thread(() =>
                    Shield.InTransaction(() =>
                        a.Modify((ref int x) => x++)));
                t.Start();
                t.Join();

                var t2 = new Thread(() =>
                    Shield.InTransaction(() =>
                        a.Modify((ref int x) => x++)));
                t2.Start();
                t2.Join();
            });
            Assert.AreEqual(0, eventCount);

            Shield.InTransaction(() => {
                a.Modify((ref int x) => x++);
            });
            Assert.AreEqual(1, eventCount);

            Thread tUnsub = null;
            Shield.InTransaction(() => {
                a.Changed.Unsubscribe(ev);
                a.Modify((ref int x) => x++);

                if (tUnsub == null)
                {
                    tUnsub = new Thread(() =>
                    {
                        Shield.InTransaction(() => {
                            a.Modify((ref int x) => x++);
                            a.Modify((ref int x) => x++);
                        });
                    });
                    tUnsub.Start();
                    tUnsub.Join();
                }
            });
            // the other thread must still see the subscription...
            Assert.AreEqual(3, eventCount);

            Shield.InTransaction(() => a.Modify((ref int x) => x++));
            Assert.AreEqual(3, eventCount);
        }

        [Test]
        public void StoppedRollbackTest()
        {
            var i = new Shielded<int>();
            int retryCount = 0;
            Shield.InTransaction(() => {
                retryCount++;
                Shield.SideEffect(null, () => {
                    var localStore = i.GetType()
                        .GetField("_locals", BindingFlags.Instance | BindingFlags.NonPublic)
                        .GetValue(i);
                    Assert.IsNull(localStore.GetType()
                        .GetField("_holderContext", BindingFlags.Instance | BindingFlags.NonPublic)
                        .GetValue(localStore));
                });
                try
                {
                    i.Value = 10;
                    if (retryCount == 1)
                        Shield.Rollback();
                }
                catch (TransException) { }
            });
            Assert.AreEqual(2, retryCount);
        }

        [Test]
        public void SideEffectDisruptsCommit()
        {
            var a = new Shielded<int>();

            using (var cont = Shield.RunToCommit(Timeout.Infinite, () => {
                a.Value = a + 1;
            }))
            {
                cont.InContext(() =>
                    Shield.SideEffect(() => { throw new Exception(); }));

                Assert.Throws<AggregateException>(
                    cont.Commit);

                Assert.IsTrue(cont.Completed);
                Assert.IsTrue(cont.Committed);
                Assert.AreEqual(1, a);
            }
        }

        [Test]
        public void SyncSideEffectDisruptsCommit()
        {
            var a = new Shielded<int>();

            using (var cont = Shield.RunToCommit(Timeout.Infinite, () => {
                a.Value = a + 1;
            }))
            {
                cont.InContext(() =>
                    Shield.SyncSideEffect(() => { throw new Exception(); }));

                Assert.Throws<AggregateException>(
                    cont.Commit);

                Assert.IsTrue(cont.Completed);
                Assert.IsFalse(cont.Committed);
                Assert.AreEqual(0, a);
            }
        }

        [Test]
        public void WhenCommittingDisruptsCommit()
        {
            var a = new Shielded<int>();

            using (Shield.WhenCommitting(a, _ => { throw new Exception(); }))
            using (var cont = Shield.RunToCommit(Timeout.Infinite, () => {
                a.Value = a + 1;
            }))
            {
                Assert.Throws<AggregateException>(
                    cont.Commit);

                Assert.IsTrue(cont.Completed);
                Assert.IsFalse(cont.Committed);
                Assert.AreEqual(0, a);
            }
        }
    }
}
