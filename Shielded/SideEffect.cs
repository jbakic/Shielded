using System;
using System.Threading.Tasks;

namespace Shielded
{
    internal class SideEffect
    {
        private Action _fx, _rollbackFx;

        public SideEffect(Action fx, Action rollbackFx)
        {
            _fx = fx;
            _rollbackFx = rollbackFx;
        }

        public bool Commit()
        {
            if (_fx != null) _fx();
            return true;
        }

        public void Rollback()
        {
            if (_rollbackFx != null) _rollbackFx();
        }
    }
}

