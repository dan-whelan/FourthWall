using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FourthWall.Controllers;
using FourthWall.Models;
using Microsoft.AspNetCore.Mvc;

namespace FourthWall.Tests
{
    [TestClass]
    public class TestFourthWallAPI
    {
        [TestMethod]
        public void testGetTableStats()
        {
            var controller = new FourthWallController();
            var result = controller.getTableStatsAPI("college", "student") as IActionResult;
            Assert.IsInstanceOfType(result, typeof(OkResult));
            result = controller.getTableStatsAPI("college", "students") as IActionResult;
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void testGetAllIndexUsage()
        {
            var controller = new FourthWallController();
            var result = controller.getAllIndexUsageAPI("college", "student") as IActionResult;
            Assert.IsInstanceOfType(result, typeof(OkResult));
            result = controller.getAllIndexUsageAPI("college", "students") as IActionResult;
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void testSpecificIndexUsage()
        {
            var controller = new FourthWallController();
            var result = controller.getIndexUsageAPI("college", "address", "idx_address_city") as IActionResult;
            Assert.IsInstanceOfType(result, typeof(OkResult));
            result = controller.getIndexUsageAPI("college", "student","idx_address_city") as IActionResult;
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void testGetTableData()
        {
            var controller = new FourthWallController();
            var result = controller.getSystemTableDataAPI("college", "auth-student") as IActionResult;
            Assert.IsInstanceOfType(result, typeof(OkResult));
            result = controller.getSystemTableDataAPI("college", "students") as IActionResult;
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void testPostExplainAnalyze()
        {
            var controller = new FourthWallController();
            var query = new Query { query = "SELECT * FROM college.student" };
            var result = controller.postExplainAnalyseAPI(query) as IActionResult;
            Assert.IsInstanceOfType(result, typeof(OkResult)); 
        }

        [TestMethod]
        public void testOpenTransactions()
        {
            var controller = new FourthWallController();
            var result = controller.getTransactions() as IActionResult;
            Assert.IsInstanceOfType(result, typeof(OkResult));
        }

        [TestMethod]
        public void testLongRunningQueries()
        {
            var controller = new FourthWallController();
            var result = controller.getQueriesAPI("202204131030000000", "202204131100000000") as IActionResult;
            Assert.IsInstanceOfType(result, typeof(OkResult));
            result = controller.getQueriesAPI(null, null) as IActionResult;
            Assert.IsInstanceOfType(result, typeof(OkResult));
        }
    }
}
