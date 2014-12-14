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
            return !t.IsClass || t.IsSealed || !t.IsPublic ||
                t.GetConstructor(Type.EmptyTypes) == null ||
                !t.GetProperties().Any(ProxyGen.IsInteresting);
        }
    }
}
