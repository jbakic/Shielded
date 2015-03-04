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
        public void NoExpandingOfTransaction()
        {
            var a = new Shielded<int>();
            var b = new Shielded<int>();
            var written = new Shielded<int>();

            using (Shield.WhenCommitting<Shielded<int>>(ints =>
                {
                    var fieldList = ints.ToList();

                    // reading what is read or written is ok.
                    Assert.IsFalse(fieldList.Contains(a));
                    int x = a + written;

                    // in written fields, you can write.
                    Assert.IsTrue(fieldList.Contains(written));
                    written.Value = written + 1;

                    Assert.IsFalse(fieldList.Contains(b));
                    try
                    {
                        // this one was not even read
                        x = b;
                        Assert.Fail("WhenCommitting subscription managed to read a new field.");
                    }
                    catch (InvalidOperationException) { }
                    try
                    {
                        // and this one was just read, and cannot be made writeable
                        a.Value = a + 1;
                        Assert.Fail("WhenCommitting subscription managed to write in a read-only field.");
                    }
                    catch (InvalidOperationException) { }
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

