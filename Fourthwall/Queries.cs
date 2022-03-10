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
        static void Main(string[] args)
        {
            //TestConnection();

            // modify the parameters below to the values of your local postgres table and schema 
            string schema = "college";
            string table = "address";
            string query = $"SELECT * FROM {schema}.{table} WHERE city = 'Kensington';";

            getTableStatistics(schema);
            getIndexStatistics(schema);
            getTableData(schema, table);
            getResultOfExplainAnalyze(query);
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
            return new NpgsqlConnection(@"Server=localhost;Port=5432;User Id=postgres;Password=[password];Database=[name]");
        }

        private static void getTableStatistics(string schema) 
        {
            //get connection
            NpgsqlConnection con = GetConnection();
            con.Open();

             // Define a query
            NpgsqlCommand command = new NpgsqlCommand($"SELECT relname, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins,n_tup_upd,n_tup_del, n_tup_hot_upd, n_live_tup,  n_dead_tup,   n_mod_since_analyze,   n_ins_since_vacuum,   last_vacuum,   last_autovacuum,   last_analyze,   last_autoanalyze,   vacuum_count,   autovacuum_count,   analyze_count,    autoanalyze_count FROM pg_stat_all_tables WHERE schemaname = '{schema}' ORDER BY relname;", con);

             // Execute the query and obtain a result set
            executeAndPrintResults(command);
            con.Close();
        } 
        

        private static void getIndexStatistics(string schema) 
        {
             //get connection
            NpgsqlConnection con = GetConnection();
            con.Open();

             // Define a query
            NpgsqlCommand command = new NpgsqlCommand($"SELECT relname,  indexrelname,   idx_scan,  idx_tup_read,  idx_tup_fetch FROM  pg_stat_all_indexes WHERE schemaname = '{schema}' ORDER BY     relname,   indexrelname;", con);

             // Execute the query and obtain a result set
            executeAndPrintResults(command);
            con.Close();
        } 
        
        // api team calls this method 
        private static void getTableData(string schema, string table) 
        {
            NpgsqlConnection con = GetConnection();
            con.Open();
            string sql = $"SELECT * FROM {schema}.{table};";
            NpgsqlCommand cmd = new NpgsqlCommand(sql, con);
            executeAndPrintResults(cmd);
            con.Close();
        }

        // api team calls this method 
        private static void getResultOfExplainAnalyze(string query) 
        {
            NpgsqlConnection con = GetConnection();
            con.Open();
            string sql = $"EXPLAIN ANALYZE {query}";
            NpgsqlCommand cmd = new NpgsqlCommand(sql, con);
            executeAndPrintResults(cmd);
            con.Close();
        }
        // Executes and Prints the given command (number of rows printed is limited to 10)
        private static void executeAndPrintResults(NpgsqlCommand cmd) 
        {
            NpgsqlDataReader reader = cmd.ExecuteReader();
            StringBuilder sb = new System.Text.StringBuilder();
            for (int i = 0; i < reader.FieldCount; i++) 
            {
                sb.Append(String.Format("{0, -24}", reader.GetName(i)));
            }
            sb.Append('\n');

            int rowCount = 0;
            while (reader.Read()) 
            {
                for (int i = 0; i < reader.FieldCount; i++) 
                {
                    sb.Append(String.Format("{0, -20}", reader[i]));
                    sb.Append("\t");
                }
                sb.Append('\n');
                if (++rowCount >= 10) break;
            }
        
            Console.WriteLine(sb);
        }
    }
    
}
