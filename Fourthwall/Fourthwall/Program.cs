using System.Text;
using Npgsql;
using System.Data;
using System.Globalization;
namespace Fourthwall 
{
    public class Program
    {
        private static string basePath = "";
        private static int longRunningQueriesDefaultTimeInSeconds;
        static void Main(string[] args)
        {
            var root = Directory.GetCurrentDirectory();
            var dotenv = Path.Combine(root, ".env");
            DotEnv.Load(dotenv);
            string? tmpBasePath = Environment.GetEnvironmentVariable("BASEPATH");
            string? timeIntervalString = Environment.GetEnvironmentVariable("POLLING_INTERVAL_MILLISECONDS");
            string? longRunningQueriesDefaultTimeInSecondsString = Environment.GetEnvironmentVariable("LONG_RUNNING_QUERIES_DEFAULT_TIME_SECS");
            if(!int.TryParse(longRunningQueriesDefaultTimeInSecondsString, out longRunningQueriesDefaultTimeInSeconds)) 
            {
                longRunningQueriesDefaultTimeInSeconds = 5;
            }
            int timeInterval;
            if (!int.TryParse(timeIntervalString, out timeInterval)) 
            {
                timeInterval = 15000;
            }
            if (tmpBasePath == null) 
            {
                basePath = "";
            } else basePath = tmpBasePath;

            while (true) 
            {
                Console.Write("Start time: ");
                Console.WriteLine(DateTime.Now.ToString("hh:mm:ss tt"));
                storeTableStatistics();
                storeIndexStatistics();
                storeLongRunningQueries();
                Console.Write("End time: ");
                Console.WriteLine(DateTime.Now.ToString("hh:mm:ss tt"));
                Console.WriteLine();
                Thread.Sleep(timeInterval);
            }

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
            // TODO Error handling here 
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
        private static void storeLongRunningQueries() 
        {
             //get connection
            NpgsqlConnection con = GetConnection();
            con.Open();

            // Define a query
            // query must be the last column in the SELECT statement due to how parsing is implemented
            NpgsqlCommand cmd = new NpgsqlCommand($@"SELECT
            pid,
            user,
            pg_stat_activity.query_start,
            age(clock_timestamp(), query_start),
            state,
            wait_event_type,
            wait_event,
            query
            FROM pg_stat_activity
            WHERE state != 'idle in transaction' AND state != 'idle in transaction (aborted)' AND (now() - pg_stat_activity.query_start) > interval '{longRunningQueriesDefaultTimeInSeconds} seconds';", con);
             
             // Execute the query and obtain a result set
            // NpgsqlDataReader reader = cmd.ExecuteReader();
            // prettyPrint(command);
            retrieveLongRunningQueriesAndPersist(cmd);
            con.Close();
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

        private static List<Dictionary<string, string>> parseQueries(string[] data) 
        {
            var list = new List<Dictionary<string, string>>();

            string[] headers = data[0].Split(',');
            string[] queries = data.Skip(1).ToArray();
            foreach (string query in queries) 
            {
                string[] stats = query.Split(',', headers.Length);
                list.Add(headers.Zip(stats, (k, v) => new {Key = k, Value = v}).ToDictionary(x => x.Key, x => x.Value));
            }

            return list;
        }

        private static Dictionary<string, string> parseExplainAnalyze(string[] data) 
        {
            string queryPlan = data[1];
            // Console.WriteLine(data.Length);
            Dictionary<string, string> map = new Dictionary<string, string>();

            map.Add("queryPlan", queryPlan);
            map.ToList().ForEach(x => Console.WriteLine(x.Value));

            return map;
        }

        private static Dictionary<string, List<Dictionary<string, string>>> parseOpenTransactions(string[] data) 
        {
            string[] headers = data[0].Split(',');

            var openTransactions = new Dictionary<string, List<Dictionary<string, string>>>();
            List<Dictionary<string, string>> readOnlyTransactions = new List<Dictionary<string, string>>();
            List<Dictionary<string, string>> readWriteTransactions = new List<Dictionary<string, string>>();

           for (int i = 1; i < data.Length; i++) 
            {
                string[] stats = data[i].Split(',', headers.Length);

                Dictionary<string, string> map = headers.Zip(stats, (k, v) => new {Key = k, Value = v}).ToDictionary(x => x.Key, x => x.Value);
                if(map["locktype"].Equals("transactionid")) 
                {
                    readWriteTransactions.Add(map);
                } 
                else if (map["locktype"].Equals("virtualxid")) 
                {
                    readOnlyTransactions.Add(map);
                }
            }

            openTransactions.Add("readOnly", readOnlyTransactions);
            openTransactions.Add("readWrite", readWriteTransactions);
            // readOnlyTransactions.ForEach(x => x.ToList().ForEach(x => Console.WriteLine(x)));
            // readWriteTransactions.ForEach(x => x.ToList().ForEach(x => Console.WriteLine(x)));

            return openTransactions;
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

        private static void retrieveLongRunningQueriesAndPersist(NpgsqlCommand cmd) 
        {
            NpgsqlDataReader reader = cmd.ExecuteReader();
            string currentTimestamp = DateTime.Now.ToString("yyyyMMddHHmmssffff");
            // Console.WriteLine(currentTimestamp);
            StringBuilder headers = new System.Text.StringBuilder();
            StringBuilder longRunningQueries = new System.Text.StringBuilder();
            for (int i = 0; i < reader.FieldCount; i++) 
            {
                headers.Append(String.Format("{0}", reader.GetName(i)));
                if (i != reader.FieldCount - 1) 
                {
                    headers.Append(',');
                }
            }

            while (reader.Read()) 
            {
                StringBuilder stats = new System.Text.StringBuilder();
                for (int i = 0; i < reader.FieldCount; i++) 
                {
                    stats.Append(String.Format("{0}", reader[i]));
                    if (i != reader.FieldCount - 1) 
                    {
                        stats.Append(',');
                    }
                }
                longRunningQueries.Append("<query>" + stats.ToString()); // <query> is the delimeter
            }

            string data = headers.ToString() + longRunningQueries.ToString();
            store($"LongRunningQueries/{currentTimestamp}", data);
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

        private static string[] retrieveResultsOfOpenTransaction(NpgsqlDataReader reader)
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

        // string time is in yyyyMMddHHmmssffff format
        public static bool isWithinTimestamp(DateTime from, DateTime to, string timeString) 
        {
            DateTime time = DateTime.ParseExact(timeString, "yyyyMMddHHmmssffff", CultureInfo.InvariantCulture);
            return from <= time && time <= to;
        }

        private static List<Dictionary<string, string>> aggregateQueries(IEnumerable<string> paths) 
        {
            var result = new List<Dictionary<string, string>>();
            foreach (string path in paths) 
            {
                Console.WriteLine(path);
                Console.WriteLine();
                string fullpath = Path.Combine(basePath, path);
                byte[] byteArr = File.ReadAllBytes(fullpath);
                string str = System.Text.Encoding.Default.GetString(byteArr);
                string[] arr = str.Split("<query>");
                var parsedQueries = parseQueries(arr);
                result.AddRange(parsedQueries);
            }
            
            result.ForEach(x => x.ToList().ForEach(x => Console.WriteLine(x)));
            return result;
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
        public static List<Dictionary<string, string>> getLongRunningQueries(DateTime fromTime, DateTime toTime) 
        {
            string fullParentDir = Path.Combine(basePath, "LongRunningQueries");
            DirectoryInfo dirInfo = new DirectoryInfo(fullParentDir);

            IEnumerable<string> paths = from f in dirInfo.EnumerateFiles()
                                        where isWithinTimestamp(fromTime, toTime, f.Name)
                                        select Path.Combine("LongRunningQueries", f.Name);
            
            return aggregateQueries(paths);
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
        public static Dictionary<string, List<Dictionary<string, string>>> getOpenTransactions() 
        { 
            NpgsqlConnection con = GetConnection();
            con.Open();
            // query must be the last column in the SELECT statement due to how parsing is implemented
            string sql = @"SELECT locktype, locks.pid, transactionid, virtualxid, state, mode, granted, xact_start, query_start, state_change, wait_event_type, wait_event, query
                            FROM pg_locks locks
                            INNER join pg_stat_activity activity
                                ON locks.transactionid = activity.backend_xid OR locks.pid = activity.pid
                            WHERE locks.locktype = 'transactionid' OR locks.locktype='virtualxid';";
            NpgsqlCommand cmd = new NpgsqlCommand(sql, con);
            NpgsqlDataReader reader = cmd.ExecuteReader();
            //prettyPrint(reader);

            string[] openTransactions = retrieveResultTableData(reader);
            Dictionary<string, List<Dictionary<string, string>>> result = parseOpenTransactions(openTransactions);
            
            con.Close();
            return result;
        }
    }    

}
