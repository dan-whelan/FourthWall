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
        [HttpGet("/api/queries?from=&to=")] // May need to be changed
        public int getQueries()
        {
            return 1;
        }

        [HttpGet("/api/transactions")]
        public int getTransactions()
        {
            return 1;
        }

        [HttpGet("/api/statistics/{schema}/{table}")]
        public int getTableStats()
        {
            return 1;
        }

        [HttpGet("/api/indexes/{schema}/{table}/{index}")]
        public int getIndexUsage()
        {
            return 1;
        }

        [HttpGet("/api/indexes/{schema}/{table}")]
        public int getAllIndexUsage()
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
        public int getSystemTableData()
        {
            return 1;
        }
    }
}