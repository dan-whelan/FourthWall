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

namespace Fourthwall 
{
    internal class Queries
    {
        private static string basePath = "C:/Users/harol/Documents";
        static void Main(string[] args)
        {
            //TestConnection();
            var root = Directory.GetCurrentDirectory();
            var dotenv = Path.Combine(root, ".env");
            DotEnv.Load(dotenv);

            // modify the parameters below to the values of your local postgres table and schema, ensure query is valid as well
            string schema = "college";
            string table1 = "student";
            string table2 = "address";
            string query = $"SELECT * FROM {schema}.{table2} WHERE city = 'Kensington';";

            storeTableStatistics();
            storeIndexStatistics();
            getTableData(schema, table1);
            getResultOfExplainAnalyze(query);
            
            store("tablestats/schemax/tablex", "it works!");
            Console.WriteLine("stored!");
            getData("tablestats/schemax/tablex");
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
            return new NpgsqlConnection($"Server=localhost;Port=5432;User Id=postgres;Password={password};Database={database}");
        }

        private static void storeTableStatistics() 
        {
            //get connection
            NpgsqlConnection con = GetConnection();
            con.Open();

             // Define a query
            NpgsqlCommand command = new NpgsqlCommand($"SELECT * FROM pg_stat_all_tables ORDER BY schemaname;", con);

             // Execute the query and obtain a result set
            executeAndPrintResults(command, 0);
            con.Close();
        } 
        

        private static void storeIndexStatistics() 
        {
             //get connection
            NpgsqlConnection con = GetConnection();
            con.Open();

             // Define a query
            NpgsqlCommand command = new NpgsqlCommand($"SELECT * FROM  pg_stat_all_indexes ORDER BY schemaname;", con);
             
             // Execute the query and obtain a result set
            executeAndPrintResults(command, 2);
            con.Close();
        } 

        private static void storeSlowRunningQueries(string fromTimestamp, string toTimestamp) 
        {

        }
        
        // overwrites the file
        private static void store(string path, string data) 
        {
            // create the directory if it does not exist
            int lastIndex = path.LastIndexOf('/');
            Directory.CreateDirectory(Path.Combine(basePath, path[..lastIndex]));

            string fullpath = Path.Combine(basePath, path);
            byte[] byteArr =  Encoding.ASCII.GetBytes(data);
            File.WriteAllBytes(fullpath, byteArr);
        }

        private static void getData(string path) 
        {
            string fullpath = Path.Combine(basePath, path);
            byte[] byteArr = File.ReadAllBytes(fullpath);
            string str = System.Text.Encoding.Default.GetString(byteArr);
            Console.WriteLine($"data = {str}");
        }

        // api team calls this method 
        private static void getTableData(string schema, string table) 
        {
            NpgsqlConnection con = GetConnection();
            con.Open();
            string sql = $"SELECT * FROM {schema}.{table};";
            NpgsqlCommand cmd = new NpgsqlCommand(sql, con);
            executeAndPrintResults(cmd, 2);
            con.Close();
        }

        // api team calls this method 
        private static void getResultOfExplainAnalyze(string query) 
        {
            NpgsqlConnection con = GetConnection();
            con.Open();
            string sql = $"EXPLAIN ANALYZE {query}";
            NpgsqlCommand cmd = new NpgsqlCommand(sql, con);
            executeAndPrintResults(cmd, 2);
            con.Close();
        }

        // api team calls this method
        private static void getOpenTransactions(string schema, string tableName) 
        {

        }

        // Executes and Prints the given command (number of rows printed is limited to 10)
        private static void executeAndPrintResults(NpgsqlCommand cmd, int sel) 
        {
            NpgsqlDataReader reader = cmd.ExecuteReader();
            StringBuilder sb = new System.Text.StringBuilder();
            int tableindex = 0;
            int schemaindex = 0;
            for (int i = 0; i < reader.FieldCount; i++) 
            {
                sb.Append(String.Format("{0, -24}", reader.GetName(i)));
                if (reader.GetName(i).Equals("schemaname")) 
                {
                    Console.WriteLine($"schemaname index = {i}");
                    schemaindex = i;
                }
                if (reader.GetName(i).Equals("relname")) 
                {
                    Console.WriteLine($"tablename index = {i}");
                    tableindex = i;
                }
            }
            sb.Append('\n');

            int rowCount = 0;
            while (reader.Read()) 
            {
                string data = "";
                string schema = "";
                string table = "";
                StringBuilder rowSB = new System.Text.StringBuilder();
                for (int i = 0; i < reader.FieldCount; i++) 
                {
                    if (i == schemaindex) 
                    {
                        schema = String.Format("{0}", reader[i]);
                    }
                    if (i == tableindex) 
                    {
                        table = String.Format("{0}", reader[i]);
                    }
                    data = String.Format("{0, -24}", reader[i]);
                    rowSB.Append(data);
                    sb.Append(data);
                }
                if (sel == 0) 
                {   
                    string path = $"tablestats/{schema}/{table}";
                    store(path, rowSB.ToString());
                }
                sb.Append('\n');
                if (++rowCount >= 10) break;
            }
        
            Console.WriteLine(sb);
        }
    }    

}
