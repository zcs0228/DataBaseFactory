using DatabaseFactory;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    class Program
    {
        static void Main(string[] args)
        {
            //string connString = @"Data Source=.;Initial Catalog=FMS;Integrated Security=True";
            //DatabaseEntity entity = new DatabaseEntity(connString, DbProviderType.MySql);
            //string sql = "select * from AccountSales where Id=@id";
            //List<MyDbParameter> list = new List<MyDbParameter>();
            //list.Add(new MyDbParameter("@id", "8083a47d-1b7e-e511-992f-24050f125c9d"));
            //entity.ExecuteNonQuery(sql);

            /**************************Oracle中Clob类型测试********************************/
            string connString = @"Data Source=orcl;user id = lcOE739999;password=Test6530;";
            DatabaseEntity entity = new DatabaseEntity(connString, DbProviderType.Oracle);
            //DatabaseEntity entity = new DatabaseEntity(connString, DbProviderType.MySql);
            //string sql = "insert into MDMTest (NM,TESTCLOB) values ('11','aaaaaass')";
            string sql = "select * from MDMXTZDB";
            DataTable result = entity.ExecuteDataTable(sql);
            string test = result.Rows.Count.ToString();
            Console.WriteLine(test);
            Console.ReadKey();
            /**************************Oracle中Clob类型测试********************************/

            /**************************测试BulkSave********************************/
            //DataTable dt = new DataTable();
            //dt.Columns.Add("a");
            //dt.Columns.Add("b");
            //dt.Columns.Add("c");
            //DataRow dr1 = dt.NewRow();
            //dr1["a"] = "1";
            //dr1["b"] = 1;
            //dr1["c"] = "12321111111";
            //dt.Rows.Add(dr1);
            //DataRow dr2 = dt.NewRow();
            //dr2["a"] = "2";
            //dr2["b"] = 2;
            //dr2["c"] = "2323df1";
            //dt.Rows.Add(dr2);
            //string connString = @"Data Source=10.24.14.197;Initial Catalog=cwbasem871;user id = lcm8719999;password=Test6530;";
            //DatabaseEntity entity = new DatabaseEntity(connString, DbProviderType.MSSQL);
            //string msg = String.Empty;
            //string[] key = { "a" };
            //DataRow row = null;
            //entity.BulkSave("A", dt, key, out row, ref msg, key);
            //Console.WriteLine(msg);
            //Console.ReadKey();
            /**************************测试BulkSave********************************/
        }
    }
}
