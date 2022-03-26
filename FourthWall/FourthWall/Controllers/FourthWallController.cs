using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FourthWall.Controllers
{
    [ApiController]
    public class FourthWallController : ControllerBase
    {
        [HttpGet("/api/queries/{from:datetime?}/{to:datetime?}")]
        public int getQueries(DateTime from = DateTime.Now.Subtract(TimeSpan.FromMinutes(5)), DateTime to = DateTime.Now)
        {
            return 1;
        }

        [HttpGet("/api/transactions")]
        public int getTransactions()
        {
            return 1;
        }

        [HttpGet("/api/statistics/{schema}/{table}")]
        public int getTableStats(String schema, String table)
        {
            return 1;
        }

        [HttpGet("/api/indexes/{schema}/{table}/{index}")]
        public int getIndexUsage(String schema, String table, String index)
        {
            return 1;
        }

        [HttpGet("/api/indexes/{schema}/{table}")]
        public int getAllIndexUsage(String schema, String table)
        {
            return 1;
        }

        [HttpPost("/api/query")]
        public int postExplainAnalyse()
        {
            //
            // ASK: how to get input on query to post?
            //
            return 1;
        }

        [HttpGet("/api/data/{schema}/{table}")]
        public int getSystemTableData(String schema, String table)
        {
            return 1;
        }
    }
}