using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Shielded.ProxyGen
{
    [global::System.Serializable]
    public class ProxyGenerationException : Exception
    {
        public ProxyGenerationException() { }
        public ProxyGenerationException(string message) : base(message) { }
        public ProxyGenerationException(string message, Exception inner) : base(message, inner) { }
        protected ProxyGenerationException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context)
            : base(info, context) { }
    }
}
