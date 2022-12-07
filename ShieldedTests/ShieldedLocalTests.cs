﻿using System;
using NUnit.Framework;
using Shielded;
using System.Threading;

namespace ShieldedTests
{
    [TestFixture]
    public class ShieldedLocalTests
    {
        [Test]
        public void ShieldedLocalBasics()
        {
            var local = new ShieldedLocal<int>();

            Assert.Throws<InvalidOperationException>(() => { var b = local.HasValue; });
            Assert.Throws<InvalidOperationException>(() => { var i = local.Value; });
            Assert.Throws<InvalidOperationException>(() => local.Value = 10);
            Assert.Throws<InvalidOperationException>(() => local.Release());

            Shield.InTransaction(() => {
                local.Value = 10;
                Assert.IsTrue(local.HasValue);
                Assert.AreEqual(10, local.Value);

                var t = new Thread(() => {
                    Shield.InTransaction(() => {
                        Assert.IsFalse(local.HasValue);
                        local.Value = 20;
                        Assert.IsTrue(local.HasValue);
                        Assert.AreEqual(20, local.Value);
                    });
                });
                t.Start();
                t.Join();

                Assert.AreEqual(10, local.Value);
                local.Release();
                Assert.IsFalse(local.HasValue);
                Assert.Throws<InvalidOperationException>(() => { var i = local.Value; });
            });
        }

        [Test]
        public void ShieldedLocalVisibility()
        {
            var x = new Shielded<int>();
            var local = new ShieldedLocal<int>();

            var didItRun = false;
            using (Shield.WhenCommitting(_ => {
                didItRun = true;
                Assert.AreEqual(10, local.Value);
            }))
            {
                Shield.InTransaction(() => {
                    x.Value = 1;
                    local.Value = 10;
                });
            }
            Assert.IsTrue(didItRun);

            using (var continuation = Shield.RunToCommit(Timeout.Infinite, () => {
                local.Value = 20;
            }))
            {
                continuation.InContext(() =>
                    Assert.AreEqual(20, local.Value));
            }

            didItRun = false;
            Shield.InTransaction(() => {
                local.Value = 30;
                Shield.SyncSideEffect(() => {
                    didItRun = true;
                    Assert.AreEqual(30, local.Value);
                });
            });
            Assert.IsTrue(didItRun);
        }


        [Test]
        public void ResetOnRollback()
        {
            var local = new ShieldedLocal<int>();
            int retryCount = 0;
            Shield.InTransaction(() => {
                retryCount++;
                Assert.False(local.HasValue);
                local.Value = 10;
                if (retryCount == 1)
                    Shield.Rollback();
            });
        }

        [Test]
        public void GetDefaultValueTest()
        {
            var local = new ShieldedLocal<int>();

            Assert.Throws<InvalidOperationException>(() => { var b = local.GetValueOrDefault(); });
            Assert.Throws<InvalidOperationException>(() => { var b = local.GetValueOrDefault(42); });

            Shield.InTransaction(() =>
            {
                Assert.AreEqual(0, local.GetValueOrDefault());
                Assert.AreEqual(42, local.GetValueOrDefault(42));

                local.Value = 10;

                Assert.AreEqual(10, local.GetValueOrDefault());
                Assert.AreEqual(10, local.GetValueOrDefault(42));
            });
        }
    }
}

