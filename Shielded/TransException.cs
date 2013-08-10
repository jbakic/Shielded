using System;

namespace Shielded
{
    internal class TransException : Exception
    {
        public TransException(string message) : base(message)
        {
        }
    }
}

