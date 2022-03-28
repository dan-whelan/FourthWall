using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;
using System.IO;
using System.Runtime.Serialization.Json;
using System.Text;
using FourthWall.Models;

namespace FourthWall.Controllers
{
    [ApiController]
    public class FourthWallController : ControllerBase
    {
        private DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(List<Dictionary<string, string>>));
        private DataContractJsonSerializer dictSerializer = new DataContractJsonSerializer(typeof(Dictionary<string, string>));
        [HttpGet("/api/queries/{from:datetime?}/{to:datetime?}")]
        public IActionResult getQueriesAPI(DateTime from, DateTime to)
        {
            try
            {
                List<Dictionary<string, string>> queries = FourthWall.Program
                    .getLongRunningQueries();
                if (queries != null)
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        serializer.WriteObject(ms, queries);
                        Console.WriteLine(Encoding.Default.GetString(ms.ToArray()));
                        return Ok(JsonSerializer.Serialize(queries));
                    }
                }
                return NotFound();
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
                return NotFound();
            }

        }

        //[HttpGet("/api/transactions")]
        //public IActionResult getTransactions()
        //{
        //    try
        //    {
        //        Dictionary<string, string> transactions = getOpenTransactions();
        //        if (transactions != null)
        //        {
        //            using (MemoryStream ms = new MemoryStream())
        //            {
        //                serializer.WriteObject(ms, transactions);
        //                Console.WriteLine(Encoding.Default.GetString(ms.ToArray()));
        //                return Ok(JsonSerializer.Serialize(transactions));
        //            }
        //        }
        //        return NotFound();

        //    }
        //    catch (Exception e)
        //    {
        //        Console.WriteLine("Error: " + e.Message);
        //        return NotFound();
        //    }
        //}

        [HttpGet("/api/statistics/{schema}/{table}")]
        public IActionResult getTableStatsAPI(string schema, string table)
        {
            try
            {
                List<Dictionary<string, string>> tableStats = FourthWall.Program.getTableStats(schema, table);
                if (tableStats != null)
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        serializer.WriteObject(ms, tableStats);
                        Console.WriteLine(Encoding.Default.GetString(ms.ToArray()));
                        return Ok(JsonSerializer.Serialize(tableStats));
                    }
                }
                return NotFound();
            }
            catch(Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
                return NotFound();
            }

        }

        [HttpGet("/api/indexes/{schema}/{table}/{index}")]
        public IActionResult getIndexUsageAPI(string schema, string table, string index)
        {
            try
            {
                List<Dictionary<string, string>> indexUsage = FourthWall.Program.getIndexUsage(schema, table, index);
                if (indexUsage != null)
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        serializer.WriteObject(ms, indexUsage);
                        Console.WriteLine(Encoding.Default.GetString(ms.ToArray()));
                        return Ok(JsonSerializer.Serialize(indexUsage));
                    }
                }
                return NotFound();
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
                return NotFound();
            }
        }

        [HttpGet("/api/indexes/{schema}/{table}")]
        public IActionResult getAllIndexUsageAPI(string schema, string table)
        {
            try
            {
                List<Dictionary<string, string>> allIndexUsage = FourthWall.Program.getIndexesUsage(schema, table);
                if (allIndexUsage != null)
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        serializer.WriteObject(ms, allIndexUsage);
                        Console.WriteLine(Encoding.Default.GetString(ms.ToArray()));
                        return Ok(JsonSerializer.Serialize(allIndexUsage));
                    }
                }
                return NotFound();
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
                return NotFound();
            }
        }

        [HttpPost("/api/query/")]
        public IActionResult postExplainAnalyseAPI([FromBody] Query q)
        {
            try
            {
                Dictionary<string, string> response = FourthWall.Program.getResultOfExplainAnalyze(q.query);
                if (response != null)
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        dictSerializer.WriteObject(ms, response);
                        Console.WriteLine(Encoding.Default.GetString(ms.ToArray()));
                        return Ok(JsonSerializer.Serialize(response));
                    }
                }
                return NotFound();
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
                return NotFound();
            }
        }

        [HttpGet("/api/data/{schema}/{table}")]
        public IActionResult getSystemTableDataAPI(string schema, string table)
        {
            try
            {
                List<Dictionary<string, string>> tableData = FourthWall.Program.getTableData(schema, table);
                if (tableData != null)
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        serializer.WriteObject(ms, tableData);
                        Console.WriteLine(Encoding.Default.GetString(ms.ToArray()));
                        return Ok(JsonSerializer.Serialize(tableData));
                    }
                }
                return NotFound();
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
                return NotFound();
            }
        }
    }
}