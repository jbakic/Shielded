using System;
using System.Threading.Tasks;

namespace Trans
{
    internal class SideEffect : IShielded
    {
        private Action _fx, _rollbackFx;

        public SideEffect(Action fx, Action rollbackFx)
        {
            _fx = fx;
            _rollbackFx = rollbackFx;
        }


        public bool CanCommit(bool strict, long writeStamp)
        {
            return true;
        }

        public bool Commit(long? writeStamp)
        {
            if (_fx != null) _fx();
            return true;
        }

        public void Rollback(long? writeStamp)
        {
            if (_rollbackFx != null) _rollbackFx();
        }

        public void TrimCopies(long smallestOpenTransactionId)
        { }

        public bool HasChanges
        {
            get
            {
                return true;
            }
        }
    }
}

