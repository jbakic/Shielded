using System;
using System.Collections.Generic;
using System.Linq;
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
            Assert.IsTrue(seq.HasAny);
            Assert.AreEqual(1, seq.Head);

            for (int i = 0; i < 20; i++)
                Assert.AreEqual(i + 1, seq [i]);

            ParallelEnumerable.Range(0, 20)
                .ForAll(n => Shield.InTransaction(
                    () => seq [n] = seq [n] + 20)
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
            Assert.IsTrue(seq4.HasAny);

            Assert.AreEqual(100,
                Shield.InTransaction(() => seq4.TakeHead()));
            Assert.AreEqual(10, seq4.Count);
            Assert.AreEqual(1, seq4.Head);
            Assert.IsTrue(seq4.HasAny);

            for (int i = 0; i < 10; i++)
            {
                Assert.AreEqual(i + 1, seq4.Head);
                Assert.AreEqual(i + 1, Shield.InTransaction(() => seq4.TakeHead()));
            }

            try
            {
                Shield.InTransaction(() => { seq4.TakeHead(); });
                Assert.Fail();
            } catch (InvalidOperationException) { }
            try
            {
                var x = seq4.Head;
                Assert.Fail();
            } catch (InvalidOperationException) { }

            Assert.IsFalse(seq4.HasAny);
            Assert.AreEqual(0, seq4.Count);
        }
    }
}

