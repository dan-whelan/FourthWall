using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;
using System.IO;
using System.Runtime.Serialization.Json;
using System.Text;
using FourthWall.Models;
using FourthWall.Attributes;
/*|-------------------------------------------------------------------------------------------|
 * FourthWall API Controller
 * 
 * Controller that enables API to make use of GET and POST requests from Monitoring System
 * 
 * NEEDS TEST SUITE
 *|-------------------------------------------------------------------------------------------|
 */
namespace FourthWall.Controllers
{
    [ApiKey]
    [ApiController]
    public class FourthWallController : ControllerBase
    {
        //serializer that allows for the conversion of type List<Dictionary<string, string>> to JSON format
        private DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(List<Dictionary<string, string>>));

        //serializer that allows for the conversion of type Dictionary<string, string> to JSON format
        private DataContractJsonSerializer dictSerializer = new DataContractJsonSerializer(typeof(Dictionary<string, string>));

        /*|--------------------------------------------------------------------------------------------|
         * getQueries()
         * 
         * GET request that takes a time period from header and returns any queries that have been 
         * running for that length of time.
         * 
         * @params
         * DateTime from: a time given by the user of when to start check of queries
         * DateTime to:   a time given by the user of when to end check of queries
         * 
         * @return
         * IActionResult: a HTTP Response to the HTTP request either Ok() containing JSON formatted file
         * or NotFound()
         *|--------------------------------------------------------------------------------------------|
         */
        [HttpGet("/api/queries/{from:datetime?}/{to:datetime?}")]
        public IActionResult getQueriesAPI(DateTime? from, DateTime? to)
        {
            if(from == null || to == null)
            {
                from = DateTime.Now.AddMinutes(-5);
                to = DateTime.Now;
            }
            try
            {
                List<Dictionary<string, string>> queries = FourthWall.Program.getLongRunningQueries();
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
        /*|--------------------------------------------------------------------------------------------|
         * getQueries()
         * 
         * GET request that returns JSON formatted result containing a list of all open transactions at 
         * the time of request
         * 
         * @params
         * Null
         * 
         * @return
         * IActionResult: a HTTP Response to the HTTP request either Ok() containing JSON formatted file
         * or NotFound()
         *|--------------------------------------------------------------------------------------------|
         */

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

        /*|--------------------------------------------------------------------------------------------|
         * getTableStats()
         * 
         * GET request that returns a JSON formatted list of all statistics related to a given schema 
         * and table taken in from request header
         * 
         * @params
         * string schema: a title of a schema in a given database
         * string table:  a title of a table in the schema given prior
         * 
         * @return
         * IActionResult: a HTTP Response to the HTTP request either Ok() containing JSON formatted file
         * or NotFound()
         *|--------------------------------------------------------------------------------------------|
         */
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


        /*|--------------------------------------------------------------------------------------------|
         * getIndexUsage()
         * 
         * GET request that returns a JSON formatted list of all usage of a specified index related to 
         * a given schema and table taken in from request header
         * 
         * @params
         * string schema: a title of a schema in a given database
         * string table:  a title of a table in the schema given prior
         * string index:  an index related to the table and schema given prior
         * 
         * @return
         * IActionResult: a HTTP Response to the HTTP request either Ok() containing JSON formatted file
         * or NotFound()
         *|--------------------------------------------------------------------------------------------|
         */
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


        /*|--------------------------------------------------------------------------------------------|
         * getAllIndexUsage()
         * 
         * GET request that Returns a JSON formatted list of all usage of all indices related to a 
         * given schema and table taken in from request header
         * 
         * @params
         * string schema: a title of a schema in a given database
         * string table:  a title of a table in the schema given prior
         * 
         * @return
         * IActionResult: a HTTP Response to the HTTP request either Ok() containing JSON formatted file
         * or NotFound()
         *|--------------------------------------------------------------------------------------------|
         */
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

        /*|--------------------------------------------------------------------------------------------|
         * postExplainAnalyse()
         * 
         * POST request that takes in a query in the body of the request and returns a JSON formatted 
         * list that contains the result of the EXPLAIN ANALYZE
         * 
         * @params
         * Query q: A PostGreSQL query taken in from the body of the request
         * 
         * e.g.
         * Content-Type: application/json
         * {"query: <query-text>;"}
         * 
         * @return
         * IActionResult: a HTTP Response to the HTTP request either Ok() containing JSON formatted file
         * or NotFound()
         *|--------------------------------------------------------------------------------------------|
         */
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

        /*|--------------------------------------------------------------------------------------------|
         * getSystemTableData()
         * 
         * Returns a JSON formatted list of all data contained within a specified table
         * 
         * @params
         * string schema: a title of a schema in a given database
         * string table:  a title of a table in the schema given prior
         * 
         * @return
         * IActionResult: a HTTP Response to the HTTP request either Ok() containing JSON formatted file
         * or NotFound()
         *|--------------------------------------------------------------------------------------------|
         */
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