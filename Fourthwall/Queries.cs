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
            getTableStatistics();
            getIndexStatistics();
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

        private static void getTableStatistics() 
        {
            //get connection
            NpgsqlConnection con = new NpgsqlConnection("Server = localhost; Port = 5432; User Id = postgres; Password=[password];Database=[name]");
            con.Open();

             // Define a query
            NpgsqlCommand command = new NpgsqlCommand("SELECT relname, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins,n_tup_upd,n_tup_del, n_tup_hot_upd, n_live_tup,  n_dead_tup,   n_mod_since_analyze,   n_ins_since_vacuum,   last_vacuum,   last_autovacuum,   last_analyze,   last_autoanalyze,   vacuum_count,   autovacuum_count,   analyze_count,    autoanalyze_count FROM pg_stat_all_tables WHERE schemaname = '[schemaname]' ORDER BY relname", con);

             // Execute the query and obtain a result set
            NpgsqlDataReader dr = command.ExecuteReader();

            //create string builder
            var sb = new System.Text.StringBuilder();

            // Output rows
            sb.Append(String.Format("|{0,10}|{1,10}|{2,10}|{3,10}|\n\n", "relname", "seq_scan", "seq_tup_read", "idx_scan"));
            while (dr.Read())
            
                sb.Append( String.Format("|{0,10}|{1,10}|{2,10}|{3,10}|\n", dr[0], dr[1], dr[2], dr[3]));
                Console.WriteLine(sb);
                con.Close();
        } 
        

        private static void getIndexStatistics() 
        {
             //get connection
            NpgsqlConnection con = new NpgsqlConnection("Server = localhost; Port = 5432; User Id = postgres; Password=[password];Database=[name]");
            con.Open();

             // Define a query
            NpgsqlCommand command = new NpgsqlCommand("SELECT relname,  indexrelname,   idx_scan,  idx_tup_read,  idx_tup_fetch FROM  pg_stat_all_indexes WHERE schemaname = '[schemaname]' ORDER BY     relname,   indexrelname;", con);

             // Execute the query and obtain a result set
            NpgsqlDataReader dr = command.ExecuteReader();

            //create string builder
            var sb = new System.Text.StringBuilder();

            // Output rows
            sb.Append(String.Format("|{0,10}|{1,10}|{2,10}|{3,10}|\n\n", "relname", "indexrelname",   "idx_scan",  "idx_tup_read"));
            while (dr.Read())
            
                sb.Append( String.Format("|{0,10}|{1,10}|{2,10}|{3,10}|\n", dr[0], dr[1], dr[2], dr[3]));
                Console.WriteLine(sb);
                con.Close();
        } 
        
        // api team calls this method 
        private static void getTableData() 
        {}

        // api team calls this method 
        private static void getResultOfExplainAnalyze() 
        {}
    }
    
}
