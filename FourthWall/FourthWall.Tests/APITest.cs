using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Web.Http.Results;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FourthWall.Controllers;
using FourthWall.Models;

namespace FourthWall.Tests
{
    [TestClass]
    public class TestFourthWallAPI
    {
        [TestMethod]
        public void testGetTableStats()
        {
            var controller = new FourthWallController();
            var result = controller.getTableStatsAPI("college", "student");
            Assert.IsInstanceOfType(result, typeof(OkResult));
        }
    }
}
