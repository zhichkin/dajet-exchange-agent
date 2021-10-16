namespace Accord.GraphQL
{
    public sealed class GraphQLSettings
    {
        public string Source { get; set; } = string.Empty; // GraphQL API URI
        public string Target { get; set; } = string.Empty; // DBMS connection string
        public string UserName { get; set; } = string.Empty; // GraphQL user name
        public string Password { get; set; } = string.Empty; // GraphQL password
        public int Periodicity { get; set; } = 60; // seconds
        public int RetryDelay { get; set; } = 300; // seconds
        public string IncomingQueue { get; set; } = "РегистрСведений.DaJetExchangeВходящаяОчередь";
    }
}