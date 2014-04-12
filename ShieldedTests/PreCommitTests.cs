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
                Shield.Rollback(false);
            });

            int transactionCount = 0;
            Task.WaitAll(
                Enumerable.Range(1, 100).Select(i => Task.Factory.StartNew(() =>
                {
                    Shield.InTransaction(() =>
                    {
                        Interlocked.Increment(ref transactionCount);
                        int a = x;
                        Thread.Sleep(5);
                        x.Assign(a + i);
                    });
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
    }
}

