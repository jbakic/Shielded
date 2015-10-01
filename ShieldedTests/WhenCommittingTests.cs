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
            using (Shield.WhenCommitting(x, hasChanges => {
                commitCount++;
                Shield.Rollback();
            }))
            {
                try
                {
                    Shield.InTransaction(() => x.Value = 5);
                }
                catch (AggregateException ex)
                {
                    Assert.IsTrue(ex.InnerExceptions[0] is InvalidOperationException);
                }
            }

            Assert.AreEqual(1, commitCount);
            Assert.AreEqual(0, x);
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
        public void DictionaryAccessExpandingTest()
        {
            var d = new ShieldedDict<int, object>();

            // various combinations - one key is either written or just read, and the
            // WhenCommitting sub tries to mess with another key, or to promote the
            // read key.

            Shield.InTransaction(() => {
                d[1] = new object();
                d[2] = new object();
            });

            // WhenCommitting does not fire unless at least something changed, so we need this
            Shielded<int> x = new Shielded<int>();

            // reader promotion to writer not allowed
            using (Shield.WhenCommitting(fs => d[2] = new object()))
            {
                Assert.Throws<AggregateException>(() =>
                    Shield.InTransaction(() => {
                        x.Value = 1;
                        var obj = d[2];
                    }));
            }
            // new read not allowed
            using (Shield.WhenCommitting(fs => { var obj = d[2]; }))
            {
                Assert.Throws<AggregateException>(() =>
                    Shield.InTransaction(() => {
                        x.Value = 1;
                        var obj = d[1];
                    }));
            }
            // new write not allowed
            using (Shield.WhenCommitting(fs => d[2] = new object()))
            {
                Assert.Throws<AggregateException>(() =>
                    Shield.InTransaction(() => {
                        x.Value = 1;
                        var obj = d[1];
                    }));
            }
            // same checks, but in situations when we did a write in the dict
            using (Shield.WhenCommitting(fs => d[2] = new object()))
            {
                Assert.Throws<AggregateException>(() =>
                    Shield.InTransaction(() => {
                        d[1] = new object();
                        var obj = d[2];
                    }));
            }
            using (Shield.WhenCommitting(fs => { var obj = d[2]; }))
            {
                Assert.Throws<AggregateException>(() =>
                    Shield.InTransaction(() => {
                        d[1] = new object();
                    }));
            }
            using (Shield.WhenCommitting(fs => d[2] = new object()))
            {
                Assert.Throws<AggregateException>(() =>
                    Shield.InTransaction(() => {
                        d[1] = new object();
                    }));
            }

            // removing should likewise be restricted
            using (Shield.WhenCommitting(fs =>
                {
                    d.Remove(1);
                    Assert.Throws<InvalidOperationException>(
                        () => d.Remove(2));
                }))
            {
                Shield.InTransaction(() => {
                    d[1] = new object();
                });
            }
            // the exception was caught, and the WhenCommiting delegate committed
            Assert.IsFalse(d.ContainsKey(1));
            Shield.InTransaction(() => d[1] = new object());

            // finally, something allowed - reading from read or written, and writing into written
            using (Shield.WhenCommitting(fs =>
                {
                    var obj = d[1];
                    var obj2 = d[2];
                    d[2] = new object();
                }))
            {
                Shield.InTransaction(() => {
                    var obj = d[1];
                    d[2] = new object();
                });
            }
        }

        [Test]
        public void WhenCommittingOneField()
        {
            var a = new Shielded<int>();
            var b = new Shielded<int>();
            using (Shield.WhenCommitting(a, _ => Assert.Fail()))
            {
                Shield.InTransaction(() => b.Value = 10);
                Assert.AreEqual(10, b);
            }
            var counter = 0;
            using (Shield.WhenCommitting(a, _ => counter++))
            {
                Shield.InTransaction(() => a.Value = 10);
                Assert.AreEqual(10, a);
                Assert.AreEqual(1, counter);
            }
        }
    }
}

