using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ExternRefForProxyTest
{
    /// <summary>
    /// Exists solely to test how the ProxyGen behaves when a field uses a type
    /// defined in another assembly.
    /// </summary>
    public class AnExternalClass
    {
        public readonly int Number;

        public AnExternalClass(int number)
        {
            Number = number;
        }
    }
}
