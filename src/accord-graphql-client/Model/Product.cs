using System;
using System.Collections.Generic;

namespace Accord.GraphQL.Model
{
    internal sealed class ProductSearchRequest
    {
        private const string PAGE_NUM = "$PAGE_NUM";
        private const string PER_PAGE = "$PER_PAGE";
        private const string DATE_UTC = "$DATE_UTC";
        private readonly string QUERY = $"query ProductSearch {{ productSearch (page:{PAGE_NUM} perPage:{PER_PAGE} "
            + $"filters: {{ dateInterval: {{ range: {{ start: \"{DATE_UTC}\" end: \"2100-01-01\" }} }} }}) "
            + $"{{ pageInfo {{ page perPage total }} "
            + $"items {{ id code name updatedAt }} }} }}";
        internal ProductSearchRequest(DateTime dateUtc, int pageNum, int perPage)
        {
            query = QUERY
                .Replace(PAGE_NUM, pageNum.ToString(), StringComparison.Ordinal)
                .Replace(PER_PAGE, perPage.ToString(), StringComparison.Ordinal)
                .Replace(DATE_UTC, dateUtc.ToString("yyyy-MM-dd"), StringComparison.Ordinal);
        }
        public string query { get; private set; }
    }
    internal sealed class ProductSearchResponse
    {
        public ProductSearchData data { get; set; }
    }
    internal sealed class ProductSearchData
    {
        public ProductSearch productSearch { get; set; }
    }
    public sealed class PageInfo
    {
        public int page { get; set; }
        public int perPage { get; set; }
        public int total { get; set; }
    }
    public sealed class Product
    {
        public string id { get; set; }
        public string code { get; set; }
        public string name { get; set; }
        public DateTime updatedAt { get; set; }
    }
    public sealed class ProductSearch
    {
        public PageInfo pageInfo { get; set; }
        public List<Product> items { get; set; } = new List<Product>();
    }
}