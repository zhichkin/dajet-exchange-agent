using Accord.GraphQL.Model;
using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Accord.GraphQL
{
    public interface IAccordGraphQLClient
    {
        IAccordGraphQLClient UseServerAddress(string address);
        IAccordGraphQLClient UseCredentials(string userName, string password);
        Task<ProductSearch> GetUpdatedProducts(DateTime utc, int pageNum, int perPage);
    }
    public sealed class AccordGraphQLClient : IAccordGraphQLClient
    {
        private const string APPLICATION_JSON = "application/json";
        private const string AUTH_TOKEN_ERROR_MESSAGE = "Failed to get auth token.";
        private const string PRODUCT_SEARCH_ERROR_MESSAGE = "Failed to get product list.";

        private HttpClient Http = new HttpClient();

        private string UserName { get; set; }
        private string Password { get; set; }
        private string Token { get; set; } // JWT token

        private DateTime IssuedTimestamp { get; set; } // UTC+0 time zone
        private DateTime ExpirationTimestamp { get; set; } // UTC+0 time zone
        private DateTime RefreshExpirationTimestamp { get; set; } // UTC+0 time zone


        public IAccordGraphQLClient UseServerAddress(string address)
        {
            Token = null;
            Http.BaseAddress = new Uri(address);
            return this;
        }
        public IAccordGraphQLClient UseCredentials(string userName, string password)
        {
            Token = null;
            UserName = userName;
            Password = password;
            return this;
        }


        private void GetErrorAndThrowException(string responseBody, string errorMessage)
        {
            ResponseErrors response;
            try
            {
                response = JsonSerializer.Deserialize<ResponseErrors>(responseBody);
            }
            catch
            {
                throw new Exception(responseBody);
            }

            if (response != null && response.errors != null && response.errors.Count > 0)
            {
                throw new InvalidOperationException(response.errors[0].message);
            }

            throw new Exception(errorMessage);
        }


        private async Task RefreshAuthToken()
        {
            if (!string.IsNullOrEmpty(Token) && ExpirationTimestamp > DateTime.UtcNow)
            {
                return;
            }

            string AUTH_REQUEST = JsonSerializer.Serialize(new TokenAuthRequest(UserName, Password));

            HttpContent requestBody = new StringContent(AUTH_REQUEST, Encoding.UTF8, APPLICATION_JSON);

            HttpResponseMessage response = await Http.PostAsync("auth/", requestBody);

            string responseBody = await response.Content.ReadAsStringAsync();

            if (!response.IsSuccessStatusCode)
            {
                GetErrorAndThrowException(responseBody, AUTH_TOKEN_ERROR_MESSAGE);
            }

            TokenAuthResponse auth = JsonSerializer.Deserialize<TokenAuthResponse>(responseBody);

            if (auth == null || auth.data == null || auth.data.tokenAuth == null)
            {
                GetErrorAndThrowException(responseBody, AUTH_TOKEN_ERROR_MESSAGE);
            }

            Token = auth.data.tokenAuth.token;
            IssuedTimestamp = DateTime.UnixEpoch + TimeSpan.FromSeconds(auth.data.tokenAuth.payload.origIat);
            ExpirationTimestamp = DateTime.UnixEpoch + TimeSpan.FromSeconds(auth.data.tokenAuth.payload.exp);
            RefreshExpirationTimestamp = DateTime.UnixEpoch + TimeSpan.FromSeconds(auth.data.tokenAuth.refreshExpiresIn);
        }

        public async Task<ProductSearch> GetUpdatedProducts(DateTime utc, int pageNum, int perPage)
        {
            await RefreshAuthToken();

            string PRODUCT_SEARCH_REQUEST = JsonSerializer.Serialize(new ProductSearchRequest(utc, pageNum, perPage));

            HttpContent requestBody = new StringContent(PRODUCT_SEARCH_REQUEST, Encoding.UTF8, APPLICATION_JSON);

            Http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("JWT", Token);

            HttpResponseMessage response = await Http.PostAsync(string.Empty, requestBody);

            string responseBody = await response.Content.ReadAsStringAsync();

            if (!response.IsSuccessStatusCode)
            {
                GetErrorAndThrowException(responseBody, AUTH_TOKEN_ERROR_MESSAGE);
            }

            ProductSearchResponse result = JsonSerializer.Deserialize<ProductSearchResponse>(responseBody);

            if (result == null || result.data == null || result.data.productSearch == null)
            {
                GetErrorAndThrowException(responseBody, PRODUCT_SEARCH_ERROR_MESSAGE);
            }

            return result.data.productSearch;
        }
    }
}