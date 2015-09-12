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
                Shield.InTransaction(() => _count.Modify((ref int c) => c--));
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

        private ShieldedSeqNc<Item> _queue = new ShieldedSeqNc<Item>();
        private int _processed;
        private const int ItemCount = 500000;
        private Barrier _barrier = new Barrier(2);

        public void Run()
        {
            Console.WriteLine("Queue test...");

            ProcessorSlot.Set(Environment.ProcessorCount - 1);
            CountTracking();
            Subscribe();

            // create ItemCount items and push them in the queue.
            Stopwatch stopwatch = new Stopwatch();
            var items = Enumerable.Range(1, ItemCount).Select(
                i => new Item() { Id = Guid.NewGuid(), Code = i, Amount = 100m * i }).ToArray();
            InitTiming();
            stopwatch.Start();
            for (int i = 0; i < ItemCount / 1; i++)
            {
                Shield.InTransaction(() => {
                    for (int j = 0; j < 1; j++)
                        _queue.Append(items[i*1 + j]);
                });
            }

            Console.WriteLine("..all items added, waiting.");
            _barrier.SignalAndWait();
            var time = stopwatch.ElapsedMilliseconds;
            Console.WriteLine(" -- completed in {0} ms.", time);
        }

        private const int _reportEvery = 10000;
        private int _lastReport;
        private DateTime _lastTime;

        private void InitTiming()
        {
            _lastReport = 0;
            _lastTime = DateTime.UtcNow;
        }

        private void CountTracking()
        {
            var last = _lastReport;
            DateTime newNow = DateTime.UtcNow;
            if (_processed >= last + _reportEvery &&
                Interlocked.CompareExchange(ref _lastReport, last + _reportEvery, last) == last)
            {
                int speed = _reportEvery * 1000 / (int)newNow.Subtract(_lastTime).TotalMilliseconds;
                _lastTime = newNow;
                int sc = _subscribeCount;
                int ptc = _processTestCount;
                int pbc = _processBodyCount;
                Console.WriteLine(
                    "{0} at {1} item/s, stats ( {2}, {3}, {4} )",
                    last + _reportEvery, speed, sc, ptc, pbc);
            }
        }

        private static int _subscribeCount;
        private static int _processTestCount;
        private static int _processBodyCount;

        private void Subscribe()
        {
            Shield.Conditional(() => ProcessorSlot.Free && _queue.Any(), () => {
                Interlocked.Increment(ref _subscribeCount);
                while (ProcessorSlot.Free)
                {
                    ProcessorSlot.Take();
                    Shield.SideEffect(() => Task.Factory.StartNew(Process));
                }
            });
        }

        private void Process()
        {
            try
            {
                ProcessInt();
            }
            catch (Exception ex)
            {
                Console.Write(" [{0}] ", ex.GetType().Name);
            }
        }

        private void ProcessInt()
        {
            int yieldCount = 0;
            Item[] items;
            while (yieldCount++ < 10)
            {
                if ((items = Shield.InTransaction(() =>
                    {
                        Interlocked.Increment(ref _processTestCount);
                        if (!_queue.Any())
                            return null;
                        return _queue.Consume.Take(10).ToArray();
                    })) != null)
                {
                    yieldCount = 0;
                    foreach (var item in items)
                    {
                        Interlocked.Increment(ref _processBodyCount);
                        // do a transaction, or whatever.
                        //if (item.Code % 10000 == 0)
                        //    Shield.SideEffect(() => Console.WriteLine("-- Item {0}", item.Code));
                        Interlocked.Increment(ref _processed);
                    }
                    CountTracking();
                }
                else
                    Thread.Yield();
            }

            Shield.InTransaction(() => {
                ProcessorSlot.Release();
                Shield.SideEffect(() => {
                    if (_processed == ItemCount && Interlocked.Increment(ref _processed) == ItemCount + 1)
                        _barrier.SignalAndWait();
                });
            });
        }
    }
}

