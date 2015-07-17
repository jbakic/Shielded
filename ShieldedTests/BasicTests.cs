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
    [TestFixture]
    public class BasicTests
    {
        [Test]
        public void TransactionSafetyTest()
        {
            Shielded<int> a = new Shielded<int>(5);
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
            // just to confirm validity of test! not really a fail if this fails.
            Assert.Greater(transactionCount, 100);
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
                    // in case Assign() becomes commutative, we use Modify() to ensure conflict.
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
        public void CommuteTest()
        {
            var a = new Shielded<int>();

            Shield.InTransaction(() => a.Commute((ref int n) => n++));
            Assert.AreEqual(1, a);

            Shield.InTransaction(() =>
            {
                Assert.AreEqual(1, a);
                a.Commute((ref int n) => n++);
                Assert.AreEqual(2, a);
            });
            Assert.AreEqual(2, a);

            Shield.InTransaction(() =>
            {
                a.Commute((ref int n) => n++);
                Assert.AreEqual(3, a);
            });
            Assert.AreEqual(3, a);

            int transactionCount = 0, commuteCount = 0;
            ParallelEnumerable.Repeat(1, 100).ForAll(i => Shield.InTransaction(() => {
                Interlocked.Increment(ref transactionCount);
                a.Commute((ref int n) => {
                    Interlocked.Increment(ref commuteCount);
                    Thread.Sleep(10); // needs this.. (running on Mono 2.10)
                    n++;
                });
            }));
            Assert.AreEqual(103, a);
            // commutes never conflict (!)
            Assert.AreEqual(100, transactionCount);
            Assert.Greater(commuteCount, 100);

            Shield.InTransaction(() => {
                a.Commute((ref int n) => n -= 3);
                a.Commute((ref int n) => n--);
            });
            Assert.AreEqual(99, a);
        }

        [Test]
        public void DegeneratingCommuteTest()
        {
            var a = new Shielded<int>();
            int numInc = 100000;
            int transactionCount = 0, commuteCount = 0;
            ParallelEnumerable.Repeat(1, numInc/2).ForAll(i => Shield.InTransaction(() => {
                Interlocked.Increment(ref transactionCount);
                a.Commute((ref int n) => {
                    Interlocked.Increment(ref commuteCount);
                    n++;
                });
                a.Commute((ref int n) => {
                    Interlocked.Increment(ref commuteCount);
                    n++;
                });
                // so, we cause it to degenerate. there was a subtle bug in enlisting which
                // would allow a degenerated commute to execute before checking the lock!
                // this test catches that bug. we also do two commutes to confirm that
                // Shield.CheckCommutes can be reentered with the same item (see comment in Enlist).
                int x = a;
            }));
            Assert.AreEqual(numInc, a);
            // degenerated commutes conflict, which means transaction will repeat. conflict
            // may be detected before the commute lambda actually gets to execute, so the
            // trans count can be greater than commute count.
            Assert.GreaterOrEqual(transactionCount, commuteCount/2);
            // classic, just to confirm there was at least one conflict.
            Assert.Greater(commuteCount, numInc);
        }

        [Test]
        public void ComplexCommute()
        {
            // some more complex commute combinations. first, with ShieldedSeq ops.
            var seq = new ShieldedSeq<int>();

            // just a test for proper commute ordering
            Shield.InTransaction(() => {
                seq.Append(1);
                seq.Append(2);
                seq.Append(3);
                seq.Remove(2);
                Assert.IsTrue(seq.HasAny);
                Assert.AreEqual(2, seq.Count);
                Assert.AreEqual(1, seq[0]);
                Assert.AreEqual(3, seq[1]);
            });

            // a test for commutability of Append()
            Shield.InTransaction(() => { seq.Clear(); });
            int transactionCount = 0;
            Thread oneTimer = null;
            Shield.InTransaction(() => {
                transactionCount++;
                seq.Append(1);
                if (oneTimer == null)
                {
                    oneTimer = new Thread(() => Shield.InTransaction(() =>
                    {
                        seq.Append(2);
                    }));
                    oneTimer.Start();
                    oneTimer.Join();
                }
            });
            Assert.AreEqual(1, transactionCount);
            Assert.AreEqual(2, seq.Count);
            // the "subthread" commited the append first, so:
            Assert.AreEqual(2, seq[0]);
            Assert.AreEqual(1, seq[1]);
            Assert.IsTrue(seq.HasAny);

            // test for a commute degeneration - reading the head of a list causes
            // appends done in the same transaction to stop being commutable. for
            // simplicity - you could continue from the head on to the tail, and it cannot
            // thus be a commute, you read it. it could, of course, be done that it still
            // is a commute, but that would make it pretty complicated.
            Shield.InTransaction(() => { seq.Clear(); seq.Append(1); seq.Append(2); });
            Assert.AreEqual(1, seq[0]);
            Assert.AreEqual(2, seq[1]);
            transactionCount = 0;
            oneTimer = null;
            Shield.InTransaction(() => {
                transactionCount++;
                Assert.AreEqual(1, seq.TakeHead());
                seq.Append(4);
                if (oneTimer == null)
                {
                    oneTimer = new Thread(() => Shield.InTransaction(() =>
                    {
                        seq.Append(3);
                    }));
                    oneTimer.Start();
                    oneTimer.Join();
                }
            });
            Assert.AreEqual(2, transactionCount);
            Assert.AreEqual(3, seq.Count);
            Assert.IsTrue(seq.HasAny);
            Assert.AreEqual(2, seq[0]);
            Assert.AreEqual(3, seq[1]);
            Assert.AreEqual(4, seq[2]);

            Shield.InTransaction(() => { seq.Clear(); seq.Append(1); seq.Append(2); });
            Assert.AreEqual(1, seq[0]);
            Assert.AreEqual(2, seq[1]);
            transactionCount = 0;
            oneTimer = null;
            Shield.InTransaction(() => {
                // if we switch the order, doesn't matter.
                transactionCount++;
                seq.Append(4);
                Assert.AreEqual(1, seq.TakeHead());
                if (oneTimer == null)
                {
                    oneTimer = new Thread(() => Shield.InTransaction(() =>
                    {
                        seq.Append(3);
                    }));
                    oneTimer.Start();
                    oneTimer.Join();
                }
            });
            Assert.AreEqual(2, transactionCount);
            Assert.AreEqual(3, seq.Count);
            Assert.IsTrue(seq.HasAny);
            Assert.AreEqual(2, seq[0]);
            Assert.AreEqual(3, seq[1]);
            Assert.AreEqual(4, seq[2]);

            // here the removal takes out the last element in the list. this absolutely cannot
            // commute, because it read from the only element's Next field, and the Seq
            // knew that it was the last element. it must conflict.
            Shield.InTransaction(() => { seq.Clear(); seq.Append(1); });
            Assert.AreEqual(1, seq[0]);
            transactionCount = 0;
            oneTimer = null;
            Shield.InTransaction(() => {
                transactionCount++;
                Assert.AreEqual(1, seq.TakeHead());
                seq.Append(3);
                if (oneTimer == null)
                {
                    oneTimer = new Thread(() => Shield.InTransaction(() =>
                    {
                        seq.Append(2);
                    }));
                    oneTimer.Start();
                    oneTimer.Join();
                }
            });
            Assert.AreEqual(2, transactionCount);
            Assert.AreEqual(2, seq.Count);
            Assert.IsTrue(seq.HasAny);
            Assert.AreEqual(2, seq[0]);
            Assert.AreEqual(3, seq[1]);


            // it is not allowed to read another Shielded from a Shielded.Commute()!
            // this greatly simplifies things. if you still want to use a value from another
            // Shielded, you must read it in main trans, forcing it's commutes to degenerate.

            var a = new Shielded<int>();
            var b = new Shielded<int>();
            try
            {
                Shield.InTransaction(() => {
                    a.Commute((ref int n) => n = 1);
                    b.Commute((ref int n) => n = a);
                });
                Assert.Fail();
            }
            catch (InvalidOperationException) {}

            try
            {
                Shield.InTransaction(() => {
                    a.Commute((ref int n) => n = 1);
                    b.Commute((ref int n) => { n = 1; a.Commute((ref int n2) => n2 = 2); });
                });
                Assert.Fail();
            }
            catch (InvalidOperationException) {}

            Shield.InTransaction(() => {
                a.Commute((ref int n) => n = 1);
                b.Commute((ref int n) => n = a);
                try
                {
                    var x = b.Value;
                    Assert.Fail();
                }
                catch (InvalidOperationException) {}
            });
        }

        [Test]
        public void CommuteInACommute()
        {
            var a = new Shielded<int>();
            var b = new Shielded<int>();

            try
            {
                // there is only one _blockEnlist, and if it is "reused" by an inner call, it
                // would get set to null when the inner commute ends. this would allow later code
                // in the outer commute to violate the access restriction.
                Shield.InTransaction(() => {
                    a.Commute((ref int aRef) => {
                        a.Commute((ref int aRef2) => aRef2++);
                        b.Value = 1;
                    });
                });
                Assert.Fail();
            }
            catch (InvalidOperationException) { }
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
    }
}

