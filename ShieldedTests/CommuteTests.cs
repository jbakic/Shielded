using System;
using NUnit.Framework;
using Shielded;
using System.Linq;
using System.Threading;

namespace ShieldedTests
{
    [TestFixture]
    public class CommuteTests
    {
        [Test]
        public void BasicCommuteTest()
        {
            var a = new Shielded<int>();

            Shield.InTransaction(() => a.Commute((ref int n) => n++));
            Assert.AreEqual(1, a);

            Shield.InTransaction(() => {
                Assert.AreEqual(1, a);
                a.Commute((ref int n) => n++);
                Assert.AreEqual(2, a);
            });
            Assert.AreEqual(2, a);

            Shield.InTransaction(() => {
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
                a.Commute((ref int n) => n *= 2);
            });
            Assert.AreEqual(200, a);
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
                int x = a;
            }));
            Assert.AreEqual(numInc, a);
            // degenerated commutes conflict, which means transaction will repeat. conflict
            // may be detected before the commute lambda actually gets to execute, so the
            // trans count can be greater than commute count.
            Assert.GreaterOrEqual(transactionCount, commuteCount/2);
            if (commuteCount == numInc)
                Assert.Inconclusive();
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
                Assert.IsTrue(seq.Any());
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
            Assert.IsTrue(seq.Any());

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
            Assert.IsTrue(seq.Any());
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
            Assert.IsTrue(seq.Any());
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
            Assert.IsTrue(seq.Any());
            Assert.AreEqual(2, seq[0]);
            Assert.AreEqual(3, seq[1]);


            // it is not allowed to read another Shielded from a Shielded.Commute()!
            // this greatly simplifies things. if you still want to use a value from another
            // Shielded, you must read it in main trans, forcing it's commutes to degenerate.

            var a = new Shielded<int>();
            var b = new Shielded<int>();
            Assert.Throws<InvalidOperationException>(() =>
                Shield.InTransaction(() => {
                    a.Commute((ref int n) => n = 1);
                    b.Commute((ref int n) => n = a);
                }));

            Assert.Throws<InvalidOperationException>(() =>
                Shield.InTransaction(() => {
                    a.Commute((ref int n) => n = 1);
                    b.Commute((ref int n) => {
                        n = 1;
                        a.Commute((ref int n2) => n2 = 2);
                    });
                }));

            Shield.InTransaction(() => {
                a.Commute((ref int n) => n = 1);
                b.Commute((ref int n) => n = a);
                Assert.Throws<InvalidOperationException>(() => {
                    var x = b.Value;
                });
            });
        }

        [Test]
        public void CommuteInACommute()
        {
            var a = new Shielded<int>();
            var b = new Shielded<int>();

            Assert.Throws<InvalidOperationException>(() =>
                // there is only one _blockEnlist, and if it is "reused" by an inner call, it
                // would get set to null when the inner commute ends. this would allow later code
                // in the outer commute to violate the access restriction.
                Shield.InTransaction(() =>
                    a.Commute((ref int aRef) => {
                        a.Commute((ref int aRef2) => aRef2++);
                        b.Value = 1;
                    })));
        }
    }
}
