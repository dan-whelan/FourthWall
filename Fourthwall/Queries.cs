using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using Npgsql;
using System.Data;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;

namespace Fourthwall 
{
    internal class Queries
    {
        private static string basePath = "";
        static void Main(string[] args)
        {
            //TestConnection();
            var root = Directory.GetCurrentDirectory();
            var dotenv = Path.Combine(root, ".env");
            DotEnv.Load(dotenv);
            string? tmpBasePath = Environment.GetEnvironmentVariable("BASEPATH");
            if (tmpBasePath == null) 
            {
                basePath = "";
            } else basePath = tmpBasePath;

            string? schema = Environment.GetEnvironmentVariable("TEST_SCHEMA_1");
            string? table1 = Environment.GetEnvironmentVariable("TEST_TABLE_1");
            string? table2 = Environment.GetEnvironmentVariable("TEST_TABLE_2");
            string? query = Environment.GetEnvironmentVariable("TEST_QUERY");

            storeTableStatistics();
            storeIndexStatistics();
            // getTableData(schema, table1);
            getResultOfExplainAnalyze(query);
            
            // test retrieving from file system
            Console.WriteLine("retrieving stats for the address table.");
            getTableStats("college", "address");

            Console.WriteLine("\nretrieving usage for the idx_address_city index.");
            getIndexUsage("college", "address", "idx_address_city");

            Console.WriteLine("\nretrieving usage for all indexes in the address table.");
            getIndexesUsage("college", "address");

            Console.WriteLine("\nretrieving usage for all indexes in student table.");
            getIndexesUsage("college", "student");
            
            Console.WriteLine("\nretrieving long running queries for timestamp1");
            getLongRunningQueries();

        }


        //tests connection between program and database
        private static void TestConnection()
        {
            using (NpgsqlConnection con = GetConnection())
            {
                con.Open();
                if (con.State == ConnectionState.Open)
                {
                    Console.WriteLine("Connected");
                }
            }
        }
        private static NpgsqlConnection GetConnection()
        {
            string? database = Environment.GetEnvironmentVariable("DATABASE");
            string? password = Environment.GetEnvironmentVariable("PASSWORD");
            string? userid = Environment.GetEnvironmentVariable("USERID");
            string? port = Environment.GetEnvironmentVariable("PORT");
            string? server = Environment.GetEnvironmentVariable("SERVER");
            return new NpgsqlConnection($"Server={server};Port={port};User Id={userid};Password={password};Database={database}");
        }

        private static void storeTableStatistics() 
        {
            //get connection
            NpgsqlConnection con = GetConnection();
            con.Open();

             // Define a query
            NpgsqlCommand cmd = new NpgsqlCommand($"SELECT * FROM pg_stat_all_tables ORDER BY schemaname;", con);

             // Execute the query and obtain a result set
            // NpgsqlDataReader reader = cmd.ExecuteReader();
            // prettyPrint(reader);
            retrieveTableStatsAndPersist(cmd);
            con.Close();
        } 
        

        private static void storeIndexStatistics() 
        {
             //get connection
            NpgsqlConnection con = GetConnection();
            con.Open();

             // Define a query
            NpgsqlCommand cmd = new NpgsqlCommand($"SELECT * FROM  pg_stat_all_indexes ORDER BY schemaname;", con);
             
             // Execute the query and obtain a result set
            // NpgsqlDataReader reader = cmd.ExecuteReader();
            // prettyPrint(command);
            retrieveIndexUsageAndPersist(cmd);
            con.Close();
        } 

        private static void storeLongRunningQueries(string fromTimestamp, string toTimestamp) 
        {

        }
        
        // note: overwrites the file
        private static void store(string path, string data) 
        {
            // create the directory if it does not exist
            int lastIndex = path.LastIndexOf('/');
            Directory.CreateDirectory(Path.Combine(basePath, path[..lastIndex]));

            string fullpath = Path.Combine(basePath, path);
            byte[] byteArr =  Encoding.ASCII.GetBytes(data);
            File.WriteAllBytes(fullpath, byteArr);
        }

        private static string[] getData(string path) 
        {
            //TODO : check if path exists if no * in it and use IFailure? see blob interface & coding standards 
            // if * ensure path before /* exists 
            // error checking for if line is an empty line at the end of the file

            char lastChar = path[path.Length-1];
            if (lastChar.Equals('*')) 
            {
                string parentDir = path.Substring(0, path.Length - 2); 
                string fullParentDir = Path.Combine(basePath, parentDir);
                string[] entries = Directory.GetFiles(fullParentDir);
                List<string> arr = new List<string>();
                for (int i = 0; i < entries.Count(); i++) 
                {
                    string entry = entries[i];
                    if (i == 0) {
                        arr.AddRange(getData(entry).ToArray());                  
                    } 
                    else arr.AddRange(getData(entry).Skip(1).ToArray());      
                }

                return arr.ToArray();
            }
            else 
            {
                string fullpath = Path.Combine(basePath, path);
                byte[] byteArr = File.ReadAllBytes(fullpath);
                string str = System.Text.Encoding.Default.GetString(byteArr);
                string[] arr = str.Split('\n');
                Array.ForEach(arr, Console.WriteLine);
                
                return arr;
            }
        }

        private static List<Dictionary<string, string>> parseData(string[] data) 
        {   
            // case 1, 1 line header, 1 line stats -> table stats, index usage 
            // case 2, 1 line header, more than 1 line stats -> long running queries 
            // case 3, 1 line name of index, 1 line header, 1 line stats, for each index in the table -> index usage for all indexes in a table 
            // note about case 3 redundant having headers for each index but keeps implementation simple. 
            // safe to assume N will never be large, where N is the number of indexes in one table 

            string[] headers = data[0].Split(',');
            var list = new List<Dictionary<string, string>>();
            for (int i = 1; i < data.Length; i++) 
            {
                string[] stats = data[i].Split(',');
                Dictionary<string, string> map = headers.Zip(stats, (k, v) => new {Key = k, Value = v}).ToDictionary(x => x.Key, x => x.Value);
                list.Add(map);
            }
            list.ForEach(x => x.ToList().ForEach(x => Console.WriteLine(x)));

            return list;
        }

        private static Dictionary<string, string> parseExplainAnalyze(string[] data) 
        {
            string queryPlan = data[1];
            // Console.WriteLine(data.Length);
            Dictionary<string, string> map = new Dictionary<string, string>();

            map.Add("queryPlan", queryPlan);
            map.ToList().ForEach(x => Console.WriteLine(x));

            return map;
        }

        private static void retrieveTableStatsAndPersist(NpgsqlCommand cmd) {
            NpgsqlDataReader reader = cmd.ExecuteReader();
            int tableindex = 0;
            int schemaindex = 0;
            string schema = "";
            string table = "";

            // get column names 
            StringBuilder headers = new System.Text.StringBuilder();
            for (int i = 0; i < reader.FieldCount; i++) 
            {
                string header = String.Format("{0}", reader.GetName(i));
                headers.Append(header);
                if (i != reader.FieldCount - 1) 
                {
                    headers.Append(',');
                }

                if (reader.GetName(i).Equals("schemaname")) 
                {
                    schemaindex = i;
                }
                else if (reader.GetName(i).Equals("relname")) 
                {
                    tableindex = i;
                }
            }           
            
            // retrieve and persist statistics for each table in the database
            while (reader.Read()) 
            {
                StringBuilder stats = new System.Text.StringBuilder();
                for (int i = 0; i < reader.FieldCount; i++) 
                {
                    if (i == schemaindex) 
                    {
                        schema = String.Format("{0}", reader[i]);
                    }
                    else if (i == tableindex) 
                    {
                        table = String.Format("{0}", reader[i]);
                    }
                    
                    stats.Append(String.Format("{0}", reader[i]));
                    if (i != reader.FieldCount - 1) 
                    {
                        stats.Append(',');
                    }
                }

                string data = headers.ToString() + '\n' + stats.ToString();
                store($"tablestats/{schema}/{table}", data);
            }
        }


        private static void retrieveIndexUsageAndPersist(NpgsqlCommand cmd) {
            NpgsqlDataReader reader = cmd.ExecuteReader();
            int tableIndex = 0;
            int schemaIndex = 0;
            int indexNameIndex = 0;
            string schema = "";
            string table = "";
            string index = "";

            // get column names 
            StringBuilder headers = new System.Text.StringBuilder();
            for (int i = 0; i < reader.FieldCount; i++) 
            {
                string header = String.Format("{0}", reader.GetName(i));
                headers.Append(header);
                if (i != reader.FieldCount - 1) 
                {
                    headers.Append(',');
                }

                if (reader.GetName(i).Equals("schemaname")) 
                {
                    schemaIndex = i;
                }
                else if (reader.GetName(i).Equals("relname")) 
                {
                    tableIndex = i;
                }
                else if (reader.GetName(i).Equals("indexrelname")) 
                {
                    indexNameIndex = i;
                }
            }           
            
            // retrieve and persist statistics for each index in the database
            while (reader.Read()) 
            {
                StringBuilder stats = new System.Text.StringBuilder();
                for (int i = 0; i < reader.FieldCount; i++) 
                {
                    if (i == schemaIndex) 
                    {
                        schema = String.Format("{0}", reader[i]);
                    }
                    else if (i == tableIndex) 
                    {
                        table = String.Format("{0}", reader[i]);
                    }
                    else if (i == indexNameIndex) 
                    {
                        index = String.Format("{0}", reader[i]);
                    }
                    
                    stats.Append(String.Format("{0}", reader[i]));
                    if (i != reader.FieldCount - 1) 
                    {
                        stats.Append(',');
                    }
                }

                string data = headers.ToString() + '\n' + stats.ToString();
                store($"indexusage/{schema}/{table}/{index}", data);
            }
        }

        private static void retrieveLongRunningQueriesAndPersist() 
        {
            
        }

        private static string[] retrieveResultExplainAnalyze(NpgsqlDataReader reader) 
        {
            List<string> tableData = new List<string>();
            StringBuilder headers = new System.Text.StringBuilder();
            StringBuilder stats = new System.Text.StringBuilder();
            for (int i = 0; i < reader.FieldCount; i++) 
            {
                headers.Append(String.Format("{0}", reader.GetName(i)));
                if (i != reader.FieldCount - 1) 
                {
                    headers.Append(',');
                }
            }
            tableData.Add(headers.ToString());

            while (reader.Read()) 
            {
                for (int i = 0; i < reader.FieldCount; i++) 
                {
                    string data = String.Format("{0}", reader[i]);
                    stats.Append(data);
                    if (i != reader.FieldCount - 1) 
                    {
                        stats.Append(',');
                    }
                }
                stats.Append('\n');
            }
            tableData.Add(stats.ToString());

            return tableData.ToArray();
        }

        private static string[] retrieveResultTableData(NpgsqlDataReader reader) 
        {
            StringBuilder headers = new System.Text.StringBuilder();
            List<string> tableData = new List<string>();
            for (int i = 0; i < reader.FieldCount; i++) 
            {
                headers.Append(String.Format("{0}", reader.GetName(i)));
                if (i != reader.FieldCount - 1) 
                {
                    headers.Append(',');
                }
            }
            tableData.Add(headers.ToString());

            while (reader.Read()) 
            {
                StringBuilder stats = new System.Text.StringBuilder();
                for (int i = 0; i < reader.FieldCount; i++) 
                {
                    string data = String.Format("{0}", reader[i]);
                    stats.Append(data);
                    if (i != reader.FieldCount - 1) 
                    {
                        stats.Append(',');
                    }
                }
                tableData.Add(stats.ToString());
            }

            return tableData.ToArray();
        }

        private static string[] retrieveResultsOpenTransaction()
        {
            return new string[0];
        }

        // Prints the given command (number of rows printed is limited to 10). For debugging purposes
        private static void prettyPrint(NpgsqlDataReader reader) 
        {
            StringBuilder sb = new System.Text.StringBuilder();
            for (int i = 0; i < reader.FieldCount; i++) 
            {
                sb.Append(String.Format("{0, -24}", reader.GetName(i)));
            }
            sb.Append('\n');

            int rowCount = 0;
            while (reader.Read()) 
            {
                StringBuilder rowSB = new System.Text.StringBuilder();
                for (int i = 0; i < reader.FieldCount; i++) 
                {
                    string data = String.Format("{0, -24}", reader[i]);
                    rowSB.Append(data);
                    sb.Append(data);
                }
                sb.Append('\n');
                if (++rowCount >= 10) break;
            }
            Console.WriteLine(rowCount);
            Console.WriteLine(sb);
        }

        // api team calls this method 
        public static List<Dictionary<string, string>> getTableStats(string schema, string table) 
        {
            string[] stats = getData($"tablestats/{schema}/{table}");
            return parseData(stats);
        }

        // api team calls this method 
        public static List<Dictionary<string, string>> getIndexUsage(string schema, string table, string index) 
        {
            string[]stats = getData($"indexusage/{schema}/{table}/{index}");
            return parseData(stats);
        }

        // api team calls this method 
        public static List<Dictionary<string, string>> getIndexesUsage(string schema, string table) 
        {
            string[] stats = getData($"indexusage/{schema}/{table}/*");
            return parseData(stats);
        }

        // api team calls this method   
        public static List<Dictionary<string, string>> getLongRunningQueries() 
        {
            string[] stats = getData($"LongRunningQueries/timestamp1");
            return parseData(stats);
        }

        // api team calls this method 
        public static List<Dictionary<string, string>> getTableData(string? schema, string? table) 
        {
            //TODO - sanitise input to prevent sql injection 
            NpgsqlConnection con = GetConnection();
            con.Open();
            string sql = $"SELECT * FROM {schema}.{table};";
            NpgsqlCommand cmd = new NpgsqlCommand(sql, con);
            NpgsqlDataReader reader = cmd.ExecuteReader();

            // prettyPrint(reader);
            String[] tableData = retrieveResultTableData(reader);
            List<Dictionary<string, string>> result = parseData(tableData);
            
            con.Close();

            return result;
        }

        // api team calls this method 
        public static Dictionary<string, string> getResultOfExplainAnalyze(string? query) 
        {
            //TODO - sanitise input to prevent sql injection 
            NpgsqlConnection con = GetConnection();
            con.Open();
            string sql = $"EXPLAIN ANALYZE {query}";
            NpgsqlCommand cmd = new NpgsqlCommand(sql, con);
            NpgsqlDataReader reader = cmd.ExecuteReader();
            //prettyPrint(reader);

            string[] queryPlan = retrieveResultExplainAnalyze(reader);
            Dictionary<string, string> result = parseExplainAnalyze(queryPlan);

            con.Close();

            return result;
        }

        // api team calls this method
        public static string[] getOpenTransactions(string schema, string tableName) 
        {
            //TODO - sanitise input to prevent sql injection 
            return new string[0];
        }
    }    

}
