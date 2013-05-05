using System;

namespace Trans
{
    internal class TransException : Exception
    {
        public TransException(string message) : base(message)
        {
        }
    }
}

