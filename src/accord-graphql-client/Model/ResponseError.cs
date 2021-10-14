using System.Collections.Generic;

namespace Accord.GraphQL.Model
{
    internal sealed class ResponseErrors
    {
        public List<ErrorInfo> errors { get; set; } = new List<ErrorInfo>();
    }
    internal sealed class ErrorInfo
    {
        public string message { get; set; }
        public List<string> path { get; set; } = new List<string>();
        public List<ErrorLocation> locations { get; set; } = new List<ErrorLocation>();
    }
    internal sealed class ErrorLocation
    {
        public int line { get; set; }
        public int column { get; set; }
    }
}