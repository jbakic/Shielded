using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;

namespace Shielded.ProxyGen
{
    class NothingToDo
    {
        public static bool With(Type t)
        {
            if (!t.IsSealed && t.IsPublic)
            {
                return !t.GetProperties().Any(q => q.IsVirtual() && q.CanRead && q.CanWrite );
            }
            return true;
        }
    }
}
