using Newtonsoft.Json;
using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;

namespace KITE_PROD_API
{
    public class Response<T> : HttpResponseMessage
    {
        private HttpResponseMessage _httpResponseMessage;
        private string _flatContent;
        private T _deserializedContent;
        public string FlatContent => _flatContent ?? (_flatContent = Content.ReadAsStringAsync().Result);
        public T DeserializedContent => _deserializedContent == null ? (_deserializedContent = JsonConvert.DeserializeObject<T>(FlatContent)) : _deserializedContent;

        public Response(HttpResponseMessage httpResponseMessage)
        {
            _httpResponseMessage = httpResponseMessage;
        }

        public new HttpContent Content => _httpResponseMessage.Content;
        public new HttpResponseHeaders Headers => _httpResponseMessage.Headers;
        public new bool IsSuccessStatusCode => _httpResponseMessage.IsSuccessStatusCode;
        public new string ReasonPhrase => _httpResponseMessage.ReasonPhrase;
        public new HttpRequestMessage RequestMessage => _httpResponseMessage.RequestMessage;
        public new HttpStatusCode StatusCode => _httpResponseMessage.StatusCode;
        public new Version Version => _httpResponseMessage.Version;
    }
}
