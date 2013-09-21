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

        private ShieldedSeq<Item> _queue = new ShieldedSeq<Item>();
        private Shielded<int> _processed = new Shielded<int>();
        private const int ItemCount = 500000;
        private Barrier _barrier = new Barrier(2);

        public void Run()
        {
            Console.WriteLine("Queue test...");

            Subscribe();

            var maxQueueCount = new Shielded<int>();
            Shield.Conditional(() => _queue.Count > maxQueueCount, () => {
                maxQueueCount.Assign(_queue.Count);
                return true;
            });

            // create ItemCount items and push them in the queue.
            Stopwatch stopwatch = new Stopwatch();
            var items = Enumerable.Range(1, ItemCount).Select(
                i => new Item() { Id = Guid.NewGuid(), Code = i, Amount = 100m * i }).ToArray();
            stopwatch.Start();
            for (int i = 0; i < ItemCount / 10; i++)
            {
                Shield.InTransaction(() => {
                    for (int j = 0; j < 10; j++)
                        _queue.Append(items[i*10 + j]);
                });
            }

            Console.WriteLine("..all items added, waiting.");
            _barrier.SignalAndWait();
            var time = stopwatch.ElapsedMilliseconds;
            Console.WriteLine(" -- completed in {0} ms, with {1} max queue count.", time, maxQueueCount.Read);
        }

        private void Subscribe()
        {
            Shield.Conditional(() => _queue.HasAny, () => {
                Shield.SideEffect(() => Task.Factory.StartNew(Process));
                return false;
            }
            );
        }


        private int _procCount = 0;

        private void Process()
        {
            if (Interlocked.Increment(ref _procCount) != 1)
                throw new ApplicationException("Multiple procesors started!");
            while (Shield.InTransaction(() => {
                if (!_queue.HasAny)
                {
                    Subscribe();
                    return false;
                }
                var item = _queue.TakeHead();
                //Shield.SideEffect(() => Console.WriteLine("Item {0}", item.Code));
                _processed.Modify((ref int n) => n++);
                if (_processed == ItemCount)
                    Shield.SideEffect(() => _barrier.SignalAndWait());
                return true;
            })) ;
            Interlocked.Decrement(ref _procCount);
        }
    }
}

