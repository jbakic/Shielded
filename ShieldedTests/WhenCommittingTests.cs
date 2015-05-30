using System;
using System.Linq;
using NUnit.Framework;
using Shielded;

namespace ShieldedTests
{
    [TestFixture]
    public class WhenCommittingTests
    {
        [Test]
        public void RollbackWhenCommitting()
        {
            var x = new Shielded<int>();
            int commitCount = 0;
            using (Shield.WhenCommitting<Shielded<int>>(ints =>
                {
                    if (commitCount++ == 0)
                        Shield.Rollback();
                }))
            {
                Shield.InTransaction(() => x.Value = 5);
            }

            Assert.AreEqual(2, commitCount);
            Assert.AreEqual(5, x);
        }

        [Test]
        public void NoExpandingOfTransaction()
        {
            var a = new Shielded<int>();
            var b = new Shielded<int>();
            var written = new Shielded<int>();

            using (Shield.WhenCommitting<Shielded<int>>(ints =>
                {
                    // reading what is read or written is ok.
                    Assert.IsFalse(ints.Contains(a));
                    int x = a + written;

                    // in written fields, you can write.
                    Assert.IsTrue(ints.Contains(written));
                    written.Value = written + 1;

                    Assert.IsFalse(ints.Contains(b));
                    // this one was not even read
                    Assert.Throws<InvalidOperationException>(() => x = b);

                    // and this one was just read, and cannot be made writeable
                    Assert.Throws<InvalidOperationException>(() => a.Value = a + 1);
                }))
            {
                Shield.InTransaction(() => {
                    int x = a;
                    written.Value = 5;
                });
            }
        }

        [Test]
        public void DictionaryExpandingProblem()
        {
            var d = new ShieldedDict<int, object>();

            using (Shield.WhenCommitting<ShieldedDict<int, object>>(ds =>
                {
                    // this should not be allowed
                    d[2] = new object();
                }))
            {
                Shield.InTransaction(() => d[1] = new object());
            }
        }
    }
}

