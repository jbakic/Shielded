using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Shielded;
using System.Diagnostics;

namespace ConsoleTests
{
    /// <summary>
    /// A simple wrapper for a BlockingQueue and a thread which consumes items
    /// from the queue as they get added. Sending messages is transactional, but
    /// non-conflicting. Transactions wanting to send a msg will conflict only if
    /// they conflict in some other Shielded fields, in which case their msgs will
    /// be ordered as their Shielded writes are.
    /// The consuming lambda is executed out of transaction.
    /// </summary>
    public class ShieldedActor<T> : IDisposable
    {
        private readonly BlockingCollection<T> _queue;
        private readonly string _name;

        public ShieldedActor(string name, Action<T> consumer)
        {
            _name = name;
            _queue = new BlockingCollection<T>();

            Task.Factory.StartNew(() => {
                try
                {
                    foreach (var item in _queue.GetConsumingEnumerable())
                        consumer(item);
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine("ShieldedActor '{0}' unhandled exception: {1}", _name, ex);
                    Dispose();
                }
            }, TaskCreationOptions.LongRunning);
        }

        public void Dispose()
        {
            _queue.CompleteAdding();
        }

        public void Send(IEnumerable<T> items)
        {
            Shield.InTransaction(() => Shield.SyncSideEffect(() => {
                foreach (var item in items)
                    _queue.Add(item);
            }));
        }

        public void Send(T item)
        {
            Shield.InTransaction(() => Shield.SyncSideEffect(() =>
                _queue.Add(item)));
        }

        public int MessagesWaiting
        {
            get
            {
                return _queue.Count;
            }
        }
    }

    public class ActorTestRun
    {
        private const int Total = 1000000;
        private int _count;
        private Barrier _barrier = new Barrier(2);

        private const int _reportEvery = 100000;
        private int _lastReport;
        private long _lastTime;
        private Stopwatch _sw;

        private void InitTiming()
        {
            _sw = Stopwatch.StartNew();
            _lastReport = 0;
            _lastTime = _sw.ElapsedMilliseconds;
        }

        private void CountTracking()
        {
            var last = _lastReport;
            if (_count >= last + _reportEvery &&
                Interlocked.CompareExchange(ref _lastReport, last + _reportEvery, last) == last)
            {
                var newNow = _sw.ElapsedMilliseconds;
                var speed = _reportEvery * 1000 / (newNow - _lastTime);
                _lastTime = newNow;
                Console.WriteLine("{0} at {1} item/s, queue length {2}", last + _reportEvery, speed, _actor.MessagesWaiting);
            }
        }

        private readonly ShieldedActor<int> _actor;

        public ActorTestRun()
        {
            _actor = new ShieldedActor<int>("Test", i => {
                // do something...
                if (Interlocked.Increment(ref _count) == Total)
                {
                    _actor.Dispose();
                    _barrier.SignalAndWait();
                }
                else
                    CountTracking();
            });
        }

        public void Start()
        {
            var taskCount = 1;
            var package = 1;
            InitTiming();
            Task.WaitAll(Enumerable.Range(1, taskCount)
                .Select(t =>
                    Task.Factory.StartNew(() => {
                        foreach (var i in Enumerable.Range(1, Total / (package * taskCount)))
                            _actor.Send(Enumerable.Range(1, package));
                    }, TaskCreationOptions.LongRunning))
                .ToArray());
            Console.WriteLine("Sending complete.");
            _barrier.SignalAndWait();
            _sw.Stop();
            Console.WriteLine("Finished in {0} ms.", _sw.ElapsedMilliseconds);
        }
    }
}

