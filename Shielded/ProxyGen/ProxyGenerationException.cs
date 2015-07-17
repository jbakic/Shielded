using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Shielded.ProxyGen
{
    /// <summary>
    /// Exception thrown when an error occurs during CodeDOM compilation.
    /// </summary>
    [global::System.Serializable]
    public class ProxyGenerationException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Shielded.ProxyGen.ProxyGenerationException"/> class.
        /// </summary>
        public ProxyGenerationException() { }

        /// <summary>
        /// Initializes a new instance of the <see cref="Shielded.ProxyGen.ProxyGenerationException"/> class.
        /// </summary>
        /// <param name="message">Message describing the error(s).</param>
        public ProxyGenerationException(string message) : base(message) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="Shielded.ProxyGen.ProxyGenerationException"/> class.
        /// </summary>
        /// <param name="message">Message describing the error(s).</param>
        /// <param name="inner">Inner exception.</param>
        public ProxyGenerationException(string message, Exception inner) : base(message, inner) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="Shielded.ProxyGen.ProxyGenerationException"/> class
        /// based on serialized information.
        /// </summary>
        protected ProxyGenerationException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context)
            : base(info, context) { }
    }
}
