using System;

namespace Accord.GraphQL.Model
{
    internal sealed class TokenAuthRequest
    {
        private const string USERNAME = "$USERNAME";
        private const string PASSWORD = "$PASSWORD";
        private readonly string AUTH_REQUEST_TEMPLATE = $"mutation {{ tokenAuth (username: \"{USERNAME}\" password: \"{PASSWORD}\") {{ token payload refreshExpiresIn }} }}";
        internal TokenAuthRequest(string userName, string password)
        {
            query = AUTH_REQUEST_TEMPLATE
                .Replace(USERNAME, userName, StringComparison.Ordinal)
                .Replace(PASSWORD, password, StringComparison.Ordinal);
        }
        public string query { get; private set; }
    }

    internal sealed class TokenAuthResponse
    {
        public TokenAuthData data { get; set; }
    }
    internal sealed class TokenAuthData
    {
        public TokenAuth tokenAuth { get; set; }
    }
    internal sealed class TokenAuth
    {
        public string token { get; set; }
        public AuthPayload payload { get; set; }
        public int refreshExpiresIn { get; set; }
    }
    internal sealed class AuthPayload
    {
        public string username { get; set; }
        public int exp { get; set; }
        public int origIat { get; set; }
        public string iss { get; set; }
    }
}