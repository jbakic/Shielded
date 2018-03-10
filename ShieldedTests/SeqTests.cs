using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NUnit.Framework;
using Shielded;

namespace ShieldedTests
{
    [TestFixture()]
    public class SeqTests
    {
        [Test()]
        public void BasicOps()
        {
            var seq = new ShieldedSeq<int>(
                Enumerable.Range(1, 20).ToArray());

            Assert.AreEqual(20, seq.Count);
            Assert.IsTrue(seq.Any());
            Assert.AreEqual(1, seq.Head);

            for (int i = 0; i < 20; i++)
                Assert.AreEqual(i + 1, seq [i]);

            Shield.InTransaction(() => {
                int i = 1;
                foreach (int x in seq)
                {
                    Assert.AreEqual(i, x);
                    i++;
                }
            });

            ParallelEnumerable.Range(0, 20)
                .ForAll(n => Shield.InTransaction(() => 
                    seq [n] = seq [n] + 20)
            );
            for (int i = 0; i < 20; i++)
                Assert.AreEqual(i + 21, seq [i]);

            Shield.InTransaction(() => {
                seq.Append(0);
                // test commute
                Assert.AreEqual(0, seq [20]);
            }
            );
            Assert.AreEqual(21, seq.Count);

            var a = Shield.InTransaction(() => seq.TakeHead());
            Assert.AreEqual(20, seq.Count);
            Assert.AreEqual(21, a);
            for (int i = 0; i < 19; i++)
                Assert.AreEqual(i + 22, seq [i]);
            Assert.AreEqual(0, seq [19]);

            Shield.InTransaction(() => {
                seq.Prepend(a);
                seq.RemoveAt(20);
            }
            );
            Assert.AreEqual(20, seq.Count);
            for (int i = 0; i < 20; i++)
                Assert.AreEqual(i + 21, seq [i]);

            Shield.InTransaction(() => seq.RemoveAll(i => (i & 1) == 1));
            Assert.AreEqual(10, seq.Count);
            for (int i = 0; i < 10; i++)
                Assert.AreEqual(i * 2 + 22, seq [i]);

            var seq2 = new ShieldedSeq<int>(
                Shield.InTransaction(() => seq.ToArray()));
            Shield.InTransaction(() => seq.RemoveAll(i => true));
            Assert.AreEqual(0, seq.Count);
            Shield.InTransaction(() => seq2.Clear());
            Assert.AreEqual(0, seq2.Count);

            var seq3 = new ShieldedSeq<int>(
                Enumerable.Range(1, 5).ToArray());
            Shield.InTransaction(() => seq3.RemoveAt(0));
            Assert.AreEqual(4, seq3.Count);
            Assert.AreEqual(2, seq3 [0]);
            Shield.InTransaction(() => seq3.RemoveAt(3));
            Assert.AreEqual(3, seq3.Count);
            Assert.AreEqual(4, seq3 [2]);
            Shield.InTransaction(() => seq3.Append(100));
            Assert.AreEqual(4, seq3.Count);
            Assert.AreEqual(100, seq3 [3]);
            Shield.InTransaction(() => seq3.RemoveAll(i => i == 100));
            Assert.AreEqual(3, seq3.Count);
            Assert.AreEqual(4, seq3 [2]);
            Shield.InTransaction(() => seq3.Append(100));
            Assert.AreEqual(4, seq3.Count);
            Assert.AreEqual(100, seq3 [3]);
        }

        [Test()]
        public void HeadTests()
        {
            var seq4 = new ShieldedSeq<int>(
                Enumerable.Range(1, 10).ToArray());
            Shield.InTransaction(() => { seq4.Prepend(100); });
            Assert.AreEqual(11, seq4.Count);
            Assert.AreEqual(100, seq4.Head);
            Assert.AreEqual(100, seq4.First());
            Assert.IsTrue(seq4.Any());

            Assert.AreEqual(100,
                Shield.InTransaction(() => seq4.TakeHead()));
            Assert.AreEqual(10, seq4.Count);
            Assert.AreEqual(1, seq4.Head);
            Assert.IsTrue(seq4.Any());

            for (int i = 0; i < 10; i++)
            {
                Assert.AreEqual(i + 1, seq4.Head);
                Assert.AreEqual(i + 1, Shield.InTransaction(() => seq4.TakeHead()));
            }

            Assert.Throws<InvalidOperationException>(() =>
                Shield.InTransaction(() => {
                    seq4.TakeHead();
                }));
            Assert.Throws<InvalidOperationException>(() => {
                var x = seq4.Head;
            });

            Assert.IsFalse(seq4.Any());
            Assert.AreEqual(0, seq4.Count);
        }

        [Test]
        public void RemoveTest()
        {
            var seq = new ShieldedSeq<int>(
                Enumerable.Range(1, 20).ToArray());
            Shield.InTransaction(() =>
                Assert.IsTrue(seq.Remove(5)));

            Assert.AreEqual(19, seq.Count);
            Assert.IsTrue(seq.Any());
            Assert.AreEqual(1, seq.Head);

            for (int i = 1; i <= 20; i++)
                if (i == 5)
                    continue;
                else
                    Assert.AreEqual(i, seq[i > 5 ? i - 2 : i - 1]);

            Shield.InTransaction(() =>
                Assert.IsTrue(seq.Remove(1)));

            Assert.AreEqual(18, seq.Count);
            Assert.IsTrue(seq.Any());
            Assert.AreEqual(2, seq.Head);

            Shield.InTransaction(() =>
                Assert.IsTrue(seq.Remove(20)));
            Shield.InTransaction(() =>
                Assert.IsFalse(seq.Remove(30)));

            Assert.AreEqual(17, seq.Count);
            Assert.IsTrue(seq.Any());
            Assert.AreEqual(2, seq.Head);
            Assert.AreEqual(19, seq[16]);
        }

        [Test]
        public void TailPassTest()
        {
            // pass by the tail - potential commute bug
            // - when you append(), and then iterate a seq,
            //   will you find the new last item missing?
            var tailPass = new ShieldedSeq<int>(
                Enumerable.Range(1, 5).ToArray());
            Shield.InTransaction(() => {
                tailPass.Append(6);
                int counter = 0;
                foreach (var i in tailPass)
                    counter++;
                Assert.AreEqual(6, counter);
            });

            Shield.InTransaction(() => {
                // this causes immediate degeneration of the Append commute.
                var h = tailPass.Any();
                tailPass.Append(7);
                int counter = 0;
                foreach (var i in tailPass)
                    counter++;
                Assert.AreEqual(7, counter);
            });

            Shield.InTransaction(() => {
                tailPass.Append(8);
                tailPass.Append(9);
                Assert.AreEqual(1, tailPass.Head);
                int counter = 0;
                foreach (var i in tailPass)
                    Assert.AreEqual(++counter, i);
                Assert.AreEqual(9, counter);
            });
            Assert.AreEqual(9, tailPass.Count);
        }

        [Test]
        public void TwoQueuesTest()
        {
            var seq1 = new ShieldedSeq<int>();
            var seq2 = new ShieldedSeq<int>();

            // conflict should happen only on the accessed sequence, if we're appending to two..

            int transactionCount = 0;
            Thread oneTimer = null;
            Shield.InTransaction(() => {
                transactionCount++;
                seq2.Append(2);
                seq1.Append(1);
                if (oneTimer == null)
                {
                    oneTimer = new Thread(() => Shield.InTransaction(() =>
                    {
                        seq2.Append(1);
                    }));
                    oneTimer.Start();
                    oneTimer.Join();
                }
                var b = seq1.Any();
            });
            Assert.AreEqual(1, transactionCount);
            Assert.AreEqual(2, seq2.Count);
            Assert.IsTrue(seq2.Any());
            Assert.AreEqual(1, seq2[0]);
            Assert.AreEqual(2, seq2[1]);

            Shield.InTransaction(() => { seq1.Clear(); seq2.Clear(); });
            transactionCount = 0;
            oneTimer = null;
            Shield.InTransaction(() => {
                transactionCount++;
                seq1.Append(1);
                seq2.Append(2);
                if (oneTimer == null)
                {
                    oneTimer = new Thread(() => Shield.InTransaction(() =>
                    {
                        seq2.Append(1);
                    }));
                    oneTimer.Start();
                    oneTimer.Join();
                }
                // the difference is here - seq2:
                var b = seq2.Any();
            });
            Assert.AreEqual(2, transactionCount);
            Assert.AreEqual(2, seq2.Count);
            Assert.IsTrue(seq2.Any());
            Assert.AreEqual(1, seq2[0]);
            Assert.AreEqual(2, seq2[1]);
        }

        [Test]
        public void InsertTest()
        {
            var emptySeq = new ShieldedSeqNc<int>();

            Shield.InTransaction(() => emptySeq.Insert(0, 1));
            Assert.IsTrue(emptySeq.Any());
            Assert.AreEqual(1, emptySeq[0]);
            Assert.Throws<InvalidOperationException>(() => emptySeq.Count());
            Shield.InTransaction(() =>
                Assert.AreEqual(1, emptySeq.Count()));

            var seq = new ShieldedSeq<int>(2, 3, 4, 6, 7);

            Shield.InTransaction(() => seq.Insert(0, 1));
            Assert.AreEqual(6, seq.Count);
            Assert.AreEqual(1, seq[0]);
            Assert.AreEqual(2, seq[1]);

            Shield.InTransaction(() => seq.Insert(4, 5));
            Assert.AreEqual(7, seq.Count);
            Assert.AreEqual(1, seq[0]);
            Assert.AreEqual(2, seq[1]);
            Assert.AreEqual(5, seq[4]);
            Assert.AreEqual(6, seq[5]);

            Shield.InTransaction(() => seq.Insert(7, 8));
            Assert.AreEqual(8, seq.Count);
            Shield.InTransaction(() => {
                for (int i=0; i < seq.Count; i++)
                    Assert.AreEqual(i + 1, seq[i]);
            });
        }

        [Test]
        public void RemoveAllWithExceptions()
        {
            var seq = new ShieldedSeq<int>(Enumerable.Range(1, 10).ToArray());
            Shield.InTransaction(() => 
                // handling the exception here means the transaction will commit.
                Assert.Throws<InvalidOperationException>(() =>
                    seq.RemoveAll(i => {
                        if (i == 5)
                            throw new InvalidOperationException();
                        return true;
                    })));
            Assert.AreEqual(5, seq[0]);
            Assert.AreEqual(6, seq.Count);
        }

        [Test]
        public void RemoveAllLastElement()
        {
            var seq = new ShieldedSeq<int>(Enumerable.Range(1, 10).ToArray());
            Shield.InTransaction(() => seq.RemoveAll(i => i == 10));
            Shield.InTransaction(() =>
                Assert.IsTrue(seq.SequenceEqual(Enumerable.Range(1, 9))));
        }
    }
}

