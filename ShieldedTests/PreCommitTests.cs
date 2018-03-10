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
            Shield.PreCommit(() => (x.Value & 1) == 1, () => {
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
                            x.Value = a + i;
                        });
                    }
                    catch (InvalidOperationException) { }
                }, TaskCreationOptions.LongRunning)).ToArray());
            Assert.AreEqual(50, preCommitFails);
            Assert.AreEqual(2550, x);
            if (transactionCount == 100)
                Assert.Inconclusive();
        }

        public class ValidationException : Exception {}

        [Test]
        public void Validation()
        {
            var list1 = new ShieldedSeq<int>(Enumerable.Range(1, 100).ToArray());
            var list2 = new ShieldedSeq<int>();

            int validationFails = 0;
            Shield.PreCommit(() => list1.Count + list2.Count != 100, () => {
                Interlocked.Increment(ref validationFails);
                throw new ValidationException();
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
                    catch (ValidationException)
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

            var barrier = new Barrier(2);

            // first, the version with no prioritization. the slow thread will repeat.
            int slowThread1Repeats = 0;
            var slowThread1 = new Thread(() => {
                barrier.SignalAndWait();
                Shield.InTransaction(() => {
                    Interlocked.Increment(ref slowThread1Repeats);
                    int a = x;
                    Thread.Sleep(100);
                    x.Value = a - 1;
                });
            });
            slowThread1.Start();

            IDisposable conditional = null;
            conditional = Shield.Conditional(() => { int i = x; return true; },
                () => {
                    barrier.SignalAndWait();
                    Thread.Yield();
                    conditional.Dispose();
                });

            foreach (int i in Enumerable.Range(1, 1000))
            {
                Shield.InTransaction(() => {
                    x.Modify((ref int a) => a++);
                });
            }
            slowThread1.Join();

            Assert.Greater(slowThread1Repeats, 1);
            Assert.AreEqual(999, x);

            // now, we introduce prioritization, using a simple lock
            var lockObj = new object();

            // this condition gets triggered just before any attempt to commit into x
            Shield.PreCommit(() => { int a = x; return true; }, () => {
                // the simplest way to block low prio writers is just:
                //lock (lockObj) { }
                // but then the actual commit happens outside of the lock and may yet
                // cause a conflict with someone just taking the lock. still, it's safer!
                // and might be good enough for cases where a repetition won't hurt.
                bool taken = false;
                Action release = () =>
                {
                    if (taken)
                    {
                        Monitor.Exit(lockObj);
                        taken = false;
                    }
                };
                // a bit of extra safety by using sync for the commit case.
                Shield.SyncSideEffect(release);
                Shield.SideEffect(null, release);

                Monitor.Enter(lockObj, ref taken);
            });

            // not yet locked, so this is ok.
            Shield.InTransaction(() => x.Value = 0);

            int slowThread2Repeats = 0;
            var slowThread2 = new Thread(() => {
                barrier.SignalAndWait();
                lock (lockObj)
                {
                    Shield.InTransaction(() =>
                    {
                        Interlocked.Increment(ref slowThread2Repeats);
                        int a = x;
                        Thread.Sleep(100);
                        x.Value = a - 1;
                    });
                }
            });
            slowThread2.Start();

            conditional = Shield.Conditional(() => { int i = x; return true; }, () => {
                barrier.SignalAndWait();
                conditional.Dispose();
            });

            foreach (int i in Enumerable.Range(1, 1000))
            {
                Shield.InTransaction(() => {
                    x.Modify((ref int a) => a++);
                });
            }
            slowThread2.Join();

            Assert.AreEqual(1, slowThread2Repeats);
            Assert.AreEqual(999, x);
        }

        [Test]
        public void CommuteInvariantProblem()
        {
            // since pre-commits have no limitations on access, they cannot safely
            // execute within the commute sub-transaction. if they would, then this
            // test would fail on the last assertion. instead, pre-commits cause commutes
            // to degenerate.

            var testField = new Shielded<int>();
            var effectField = new Shielded<int>();
            var failCommitCount = new Shielded<int>();
            var failVisibleCount = 0;

            // check if the effect field was written to, that the testField is even.
            Shield.PreCommit(() => effectField > 0, () => {
                if ((testField & 1) == 1)
                {
                    Interlocked.Increment(ref failVisibleCount);
                    // this will always fail to commit, confirming that the transaction
                    // is already bound to fail. but, the failVisibleCount might be >0.
                    failCommitCount.Modify((ref int n) => n++);
                }
            });

            var thread = new Thread(() => {
                // if the testField is even, increment the effectField commutatively.
                foreach (int i in Enumerable.Range(1, 1000))
                    Shield.InTransaction(() => {
                        if ((testField & 1) == 0)
                        {
                            effectField.Commute((ref int n) => n++);
                        }
                    });
            });
            thread.Start();

            foreach (int i in Enumerable.Range(1, 1000))
                Shield.InTransaction(() => {
                    testField.Modify((ref int n) => n++);
                });
            thread.Join();

            Assert.AreEqual(0, failCommitCount);
            Assert.AreEqual(0, failVisibleCount);
        }
    }
}

