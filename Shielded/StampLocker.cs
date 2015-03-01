using System;
using System.Threading;

namespace Shielded
{
    class StampLocker
    {
        public void WaitUntil(Func<bool> test)
        {
            SpinWait sw = new SpinWait();
            int count = 0;
            do
            {
                sw.SpinOnce();
                if (test())
                    return;
            } while (!sw.NextSpinWillYield || ++count < 4);
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

