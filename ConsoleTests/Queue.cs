using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using Shielded;

namespace ConsoleTests
{
    public class Queue
    {
        private class Item
        {
            public Guid Id;
            public int Code;
            public decimal Amount;
        }

        private static class ProcessorSlot
        {
            private static Shielded<int> _count = new Shielded<int>();

            public static bool Free
            {
                get
                {
                    return _count > 0;
                }
            }

            public static void Take()
            {
                Shield.InTransaction(() => {
                    if (_count == 0)
                        throw new InvalidOperationException();
                    _count.Modify((ref int c) => c--);
                });
            }

            public static void Release()
            {
                Shield.InTransaction(() => _count.Modify((ref int c) => c++));
            }

            public static void Set(int level)
            {
                Shield.InTransaction(() => _count.Modify((ref int c) => c = level));
            }
        }

        private ShieldedSeq<Item> _queue = new ShieldedSeq<Item>();
        private Shielded<int> _processed = new Shielded<int>();
        private const int ItemCount = 500000;
        private Barrier _barrier = new Barrier(2);

        public void Run()
        {
            Console.WriteLine("Queue test...");

            ProcessorSlot.Set(Environment.ProcessorCount - 1);
            CountTracking();
            Subscribe();

            var maxQueueCount = new Shielded<int>();
            Shield.Conditional(() => _queue.Count > maxQueueCount, () => {
                maxQueueCount.Value = _queue.Count;
            });

            // create ItemCount items and push them in the queue.
            Stopwatch stopwatch = new Stopwatch();
            var items = Enumerable.Range(1, ItemCount).Select(
                i => new Item() { Id = Guid.NewGuid(), Code = i, Amount = 100m * i }).ToArray();
            stopwatch.Start();
            for (int i = 0; i < ItemCount / 100; i++)
            {
                Shield.InTransaction(() => {
                    for (int j = 0; j < 100; j++)
                        _queue.Append(items[i*100 + j]);
                });
            }

            Console.WriteLine("..all items added, waiting.");
            _barrier.SignalAndWait();
            var time = stopwatch.ElapsedMilliseconds;
            Console.WriteLine(" -- completed in {0} ms, with {1} max queue count.", time, maxQueueCount.Value);
        }

        private void CountTracking()
        {
            int reportEvery = 10000;
            Shielded<int> lastReport = new Shielded<int>(0);
            Shielded<DateTime> lastTime = new Shielded<DateTime>(DateTime.UtcNow);

            Shield.Conditional(() => _processed >= lastReport + reportEvery, () =>
            {
                DateTime newNow = DateTime.UtcNow;
                int count = _processed;
                int speed = (count - lastReport) * 1000 / (int)newNow.Subtract(lastTime).TotalMilliseconds;
                lastTime.Value = newNow;
                lastReport.Modify((ref int n) => n += reportEvery);
                int sc = _subscribeCount;
                int ptc = _processTestCount;
                int pbc = _processBodyCount;
                Shield.SideEffect(() =>
                {
                    Console.WriteLine(
                        "{0} at {1} item/s, stats ( {2}, {3}, {4} )",
                        count, speed, sc, ptc, pbc);
                });
            });
        }

        private static int _subscribeCount;
        private static int _processTestCount;
        private static int _processBodyCount;

        private void Subscribe()
        {
            Shield.Conditional(() => ProcessorSlot.Free && _queue.HasAny, () => {
                Interlocked.Increment(ref _subscribeCount);
                ProcessorSlot.Take();
                var first = _queue.TakeHead();
                Shield.SideEffect(() => Task.Factory.StartNew(() => Process(first)));
            });
        }

        private void Process(Item item)
        {
            do
            {
                Shield.InTransaction(() => {
                    Interlocked.Increment(ref _processBodyCount);
                    //if (item.Code % 10000 == 0)
                    //    Shield.SideEffect(() => Console.WriteLine("-- Item {0}", item.Code));
                    _processed.Commute((ref int n) => n++);
                });
            } while ((item = Shield.InTransaction(() => {
                Interlocked.Increment(ref _processTestCount);
                if (!_queue.HasAny)
                {
                    ProcessorSlot.Release();
                    return null;
                }
                return _queue.TakeHead();
            })) != null);

            Shield.InTransaction(() => {
                if (_processed == ItemCount)
                {
                    _processed.Modify((ref int p) => p++);
                    Shield.SideEffect(() => _barrier.SignalAndWait());
                }
            });
        }
    }
}

