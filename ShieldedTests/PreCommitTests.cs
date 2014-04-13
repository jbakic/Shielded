using System;
using NUnit.Framework;
using Shielded;
using System.Threading.Tasks;
using System.Linq;
using System.Threading;

namespace ShieldedTests
{
    [TestFixture]
    public class PreCommitTests
    {
        [Test]
        public void NoOdds()
        {
            var x = new Shielded<int>();

            int preCommitFails = 0;
            // we will not allow any odd number to be committed into x.
            Shield.PreCommit(() => (x.Read & 1) == 1, () => {
                Interlocked.Increment(ref preCommitFails);
                throw new InvalidOperationException();
            });

            int transactionCount = 0;
            Task.WaitAll(
                Enumerable.Range(1, 100).Select(i => Task.Factory.StartNew(() =>
                {
                    try
                    {
                        Shield.InTransaction(() =>
                        {
                            Interlocked.Increment(ref transactionCount);
                            int a = x;
                            Thread.Sleep(5);
                            x.Assign(a + i);
                        });
                    }
                    catch (InvalidOperationException) { }
                }, TaskCreationOptions.LongRunning)).ToArray());
            Assert.AreEqual(50, preCommitFails);
            Assert.AreEqual(2550, x);
            // just to confirm validity of test! not really a fail if this fails.
            Assert.Greater(transactionCount, 100);
        }

        [Test]
        public void Validation()
        {
            var list1 = new ShieldedSeq<int>(Enumerable.Range(1, 100).ToArray());
            var list2 = new ShieldedSeq<int>();

            int validationFails = 0;
            Shield.PreCommit(() => list1.Count + list2.Count != 100, () => {
                Interlocked.Increment(ref validationFails);
                throw new InvalidOperationException("Lost an item?");
            });

            int transactionCount = 0;
            Task.WaitAll(
                Enumerable.Range(1, 100).Select(i => Task.Factory.StartNew(() =>
                {
                    try
                    {
                        Shield.InTransaction(() =>
                        {
                            Interlocked.Increment(ref transactionCount);
                            int x = list1.TakeHead();
                            if (i < 100)
                                list2.Append(x);
                        });
                        Assert.AreNotEqual(100, i);
                    }
                    catch (InvalidOperationException)
                    {
                        Assert.AreEqual(100, i);
                    }
                }, TaskCreationOptions.LongRunning)).ToArray());
            Assert.AreEqual(1, validationFails);
            Assert.AreEqual(1, list1.Count);
            Assert.AreEqual(99, list2.Count);
        }

        [Test]
        public void Prioritization()
        {
            var x = new Shielded<int>();

            int slowThread1Repeats = 0;
            var slowThread1 = new Thread(() => {
                Shield.InTransaction(() => {
                    Interlocked.Increment(ref slowThread1Repeats);
                    int a = x;
                    Thread.Sleep(500);
                    x.Assign(a - 1);
                });
            });
            slowThread1.Start();

            foreach (int i in Enumerable.Range(1, 100))
            {
                Shield.InTransaction(() => {
                    x.Modify((ref int a) => a++);
                });
            }
            slowThread1.Join();

            Assert.Greater(slowThread1Repeats, 1);
            Assert.AreEqual(99, x);

            // now, we introduce prioritization.
            // this condition gets triggered before any attempt to write into x
            int ownerThreadId = -1;
            Shield.PreCommit(() => { int a = x; return true; }, () => {
                var threadId = ownerThreadId;
                if (threadId > -1 && threadId != Thread.CurrentThread.ManagedThreadId)
                    // we'll cause lower prio threads to busy wait. we could also
                    // add, e.g., an onRollback SideEffect which would wait for
                    // a certain signal before continuing the next iteration..
                    // (NB that Shield.SideEffect would, of course, have to be called
                    // before calling Rollback.)
                    Shield.Rollback();
            });

            // this will pass due to ownerThreadId == -1
            Shield.InTransaction(() => x.Assign(0));

            int slowThread2Repeats = 0;
            var slowThread2 = new Thread(() => {
                try
                {
                    Interlocked.Exchange(ref ownerThreadId, Thread.CurrentThread.ManagedThreadId);
                    Shield.InTransaction(() => {
                        Interlocked.Increment(ref slowThread2Repeats);
                        int a = x;
                        Thread.Sleep(500);
                        x.Assign(a - 1);
                    });
                }
                finally
                {
                    Interlocked.Exchange(ref ownerThreadId, -1);
                }
            });
            slowThread2.Start();

            foreach (int i in Enumerable.Range(1, 100))
            {
                Shield.InTransaction(() => {
                    x.Modify((ref int a) => a++);
                });
            }
            slowThread2.Join();

            Assert.AreEqual(1, slowThread2Repeats);
            Assert.AreEqual(99, x);
        }
    }
}

