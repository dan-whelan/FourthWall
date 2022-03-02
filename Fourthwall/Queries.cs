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
            TestConnection();
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
            return new NpgsqlConnection(@"Server=localhost;Port=5432;User Id=postgres;Password={password};Database={database}");
        }

        private static void getTableStatistics() 
        {} 

        private static void getIndexStatistics() 
        {} 
        
        // api team calls this method 
        private static void getTableData() 
        {}

        // api team calls this method 
        private static void getResultOfExplainAnalyze() 
        {}
    }
    
}
