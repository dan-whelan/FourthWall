using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
namespace FourthWall.Attributes
{
    [AttributeUsage(validOn: AttributeTargets.Class)]
    public class ApiKeyAttribute : Attribute, IAsyncActionFilter
    {
        private const string APIKEY = "ApiKey";
        public async Task OnActionExecutionAsync(ActionExecutingContext ctxt, ActionExecutionDelegate nxt)
        {
            if(!ctxt.HttpContext.Request.Headers.TryGetValue(APIKEY, out var userApiKey))
            {
                ctxt.Result = new ContentResult()
                {
                    StatusCode = 401,
                    Content = "API key not provided"
                };
                return;
            }

            var apiSettings = ctxt.HttpContext.RequestServices.GetRequiredService<IConfiguration>();

            var apiKey = apiSettings.GetValue<string>(APIKEY);

            if(!apiKey.Equals(userApiKey))
            {
                ctxt.Result = new ContentResult()
                {
                    StatusCode = 401,
                    Content = "Unauthorized Request"
                };
                return;
            }
            await nxt();
        }
    }
}
