using System;
using System.Threading;

namespace Shielded
{
    class StampLocker
    {
        public void WaitUntil(Func<bool> test)
        {
            if (test())
                return;
            SpinWait sw = new SpinWait();
            do
            {
                if (test())
                    return;
                sw.SpinOnce();
            } while (!sw.NextSpinWillYield);
            lock (this)
            {
                while (!test())
                    Monitor.Wait(this);
            }
        }

        public void Release()
        {
            lock (this)
                Monitor.PulseAll(this);
        }
    }
}

