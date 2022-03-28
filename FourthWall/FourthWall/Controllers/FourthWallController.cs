using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;
using System.IO;
using System.Runtime.Serialization.Json;
using System.Text;

namespace FourthWall.Controllers
{
    [ApiController]
    public class FourthWallController : ControllerBase
    {
        private DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(List<Dictionary<string, string>>));
        private DataContractJsonSerializer dictSerializer = new DataContractJsonSerializer(typeof(Dictionary<string, string>));
        [HttpGet("/api/queries/{from:datetime?}/{to:datetime?}")]
        public IActionResult getQueries(DateTime from, DateTime to)
        {
            try
            {
                List<Dictionary<string, string>> queries = getQueries(from, to);
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
        public IActionResult getTableStats(string schema, string table)
        {
            try
            {
                List<Dictionary<string, string>> tableStats = getTableStats(schema, table);
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
        public IActionResult getIndexUsage(string schema, string table, string index)
        {
            try
            {
                List<Dictionary<string, string>> indexUsage = getIndexUsage(schema, table, index);
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
        public IActionResult getAllIndexUsage(string schema, string table)
        {
            try
            {
                List<Dictionary<string, string>> allIndexUsage = getIndexesUsage(schema, table);
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
        public IActionResult postExplainAnalyse([FromBody]string query)
        {
            try
            {
                Dictionary<string, string> response = getResultOfExplainAnalyse(query);
                if (response != null)
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        serializer.WriteObject(ms, response);
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
        public IActionResult getSystemTableData(string schema, string table)
        {
            try
            {
                List<Dictionary<string, string>> tableData = getTableData(schema, table);
                if (tableData != null)
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        dictSerializer.WriteObject(ms, tableData);
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