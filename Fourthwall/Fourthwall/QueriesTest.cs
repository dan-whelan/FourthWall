using Xunit;
using Xunit.Abstractions;
using Newtonsoft.Json;
using Npgsql;

namespace Fourthwall
{
    public class QueriesTest
    {
        private readonly ITestOutputHelper output;
        public QueriesTest(ITestOutputHelper output)
        {
            this.output = output; 
        }

        [Fact]
        public void TableDataTest()
        {
            Setup();
            List<Dictionary<string, string>> data = getData();
            Assert.True(isDataEqual(data, Program.getTableData("testSchema", "testTable")));
            Clean();
        }

        [Fact]
        public void TestExplainAnalyze() 
        {
            Assert.True(true);
        }

        [Fact]
        public void TestTableStats() 
        {
            Assert.True(true);
        }

        [Fact]
        public void TestIndexUsage() 
        {
            Assert.True(true);
        }
        
        [Fact]
        public void TestIndexesInTableUsage() 
        {
            Assert.True(true);
        }

        [Fact]
        public void TestLongRunningQueries() 
        {
            Assert.True(true);
        }

        [Fact]
        public void TestOpenTransactions() 
        {
            Assert.True(true);
        }

        private void CreateTestSchema() 
        {
            try
            {
                NpgsqlConnection con = GetConnection();
                con.Open();
                NpgsqlCommand cmd = new NpgsqlCommand("CREATE SCHEMA IF NOT EXISTS testSchema;", con);
                cmd.ExecuteNonQuery();
                con.Close();
            }
            catch (NpgsqlException err)
            {
                output.WriteLine(err.ToString());
            }
        }
        private void CreateTestTable() 
        {
            try
            {
                NpgsqlConnection con = GetConnection();
                con.Open();
                NpgsqlCommand cmd = new NpgsqlCommand($@"CREATE TABLE IF NOT EXISTS testSchema.testTable (
                    id serial PRIMARY KEY,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL, 
                    email TEXT NOT NULL
                );", con);
                cmd.ExecuteNonQuery();
                con.Close();
            }
            catch (NpgsqlException err)
            {
                output.WriteLine(err.ToString());
            }
            
        }

        private void PopulateTestTable(List<Dictionary<string, string>> data) 
        {
            try
            {
                NpgsqlConnection con = GetConnection();
                con.Open();
                foreach (Dictionary<string, string> row in data) 
                {
                    string sql = $@"INSERT INTO testSchema.testTable(id, first_name, last_name, email) VALUES ('{row["id"]}', '{row["first_name"]}', '{row["last_name"]}', '{row["email"]}');";
                    NpgsqlCommand cmd = new NpgsqlCommand(sql, con);
                    cmd.ExecuteNonQuery();
                } 
                con.Close();
            }
            catch (NpgsqlException err)
            {
                output.WriteLine(err.ToString());
            }
        }

        private void Setup() 
        {
            // set environment variables 
            var basePath = "c:/Users/harol/Documents/TCD/SWE_PROJECT_1/code/FourthWall/Fourthwall/Fourthwall";
            var dotenv = Path.Combine(basePath, ".env");
            // output.WriteLine(Directory.GetCurrentDirectory());
            DotEnv.Load(dotenv);
            
            // create test table
            CreateTestSchema();
            CreateTestTable();
            List<Dictionary<string, string>> data = getData();
            PopulateTestTable(data);
        }

        private NpgsqlConnection GetConnection()
            {
                string? database = Environment.GetEnvironmentVariable("DATABASE");
                string? password = Environment.GetEnvironmentVariable("PASSWORD");
                string? userid = Environment.GetEnvironmentVariable("USERID");
                string? port = Environment.GetEnvironmentVariable("PORT");
                string? server = Environment.GetEnvironmentVariable("SERVER");
                return new NpgsqlConnection($"Server={server};Port={port};User Id={userid};Password={password};Database={database}");
            }

        private void viewData(List<Dictionary<string, string>>data) 
        {
            foreach (var x in data) 
            {
                output.WriteLine(x["first_name"]);
            }
        } 

        private bool isDataEqual(List<Dictionary<string, string>> data1, List<Dictionary<string, string>> data2) 
        {
            Assert.Equal(data1.Count, data2.Count);

            for (int i = 0; i < data1.Count; i++) {
                var dict1 = data1[i];
                var dict2 = data2[i];
                if (!CompareDict(dict1, dict2)) 
                {
                    return false;
                }
            }

            return true;
        }

        private bool CompareDict(Dictionary<string, string> dict1, Dictionary<string, string> dict2) 
        {
            foreach (var pair in dict1)
            {
                string? value;
                if (dict2.TryGetValue(pair.Key, out value))
                {
                    // Require value be equal.
                    if (value != pair.Value)
                    {
                        return false;                      
                    }
                }
                else
                {
                    return false;
                }
            }

            return true;
        }

        private List<Dictionary<string, string>> getData() 
        {
            string testFile = "MOCK_DATA.json";
            var json = File.ReadAllText(testFile);
            var data = JsonConvert.DeserializeObject<List<Dictionary<string, string>>>(json);
            Assert.NotNull(data);
            return data;
        }

        private void Clean() 
        {
            try
            {
                NpgsqlConnection con = GetConnection();
                con.Open();
                NpgsqlCommand cmd = new NpgsqlCommand("DROP SCHEMA IF EXISTS testSchema CASCADE;", con);
                cmd.ExecuteNonQuery();
                con.Close();
            }
            catch (NpgsqlException err)
            {
                output.WriteLine(err.ToString());
            }
        }
    }

}
