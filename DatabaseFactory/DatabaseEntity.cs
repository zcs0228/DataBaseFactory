using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseFactory
{
    public sealed class DatabaseEntity
    {
        public string ConnectionString { get; set; }
        private DbProviderFactory providerFactory;
        /// <summary>  
        /// 构造函数  
        /// </summary>  
        /// <param name="connectionString">数据库连接字符串</param>  
        /// <param name="providerType">数据库类型枚举，参见<paramref name="providerType"/></param>  
        public DatabaseEntity(string connectionString, DbProviderType providerType)
        {
            ConnectionString = connectionString;
            providerFactory = ProviderFactory.GetDbProviderFactory(providerType);
            if (providerFactory == null)
            {
                throw new ArgumentException("Can't load DbProviderFactory for given value of providerType");
            }
        }

        /// <summary>
        /// 对数据库执行增删改操作，返回受影响的行数
        /// </summary>
        /// <param name="sql">要执行的增删改的SQL语句</param>
        /// <returns></returns>
        public int ExecuteNonQuery(string sql, CommandType commandType = CommandType.Text)
        {
            return ExecuteNonQuery(sql, new List<MyDbParameter>(), commandType);
        }
        /// <summary>     
        /// 对数据库执行增删改操作，返回受影响的行数。     
        /// </summary>     
        /// <param name="sql">要执行的增删改的SQL语句</param>     
        /// <param name="parameters">执行增删改语句所需要的参数</param>  
        /// <param name="commandType">执行的SQL语句的类型</param>  
        /// <returns></returns>  
        public int ExecuteNonQuery(string sql, IList<MyDbParameter> parameters, CommandType commandType = CommandType.Text)
        {
            using (DbCommand command = DbHelper.CreateDbCommand(providerFactory, ConnectionString, sql, parameters, commandType))
            {
                command.Connection.Open();
                int affectedRows = command.ExecuteNonQuery();
                command.Connection.Close();
                return affectedRows;
            }
        }

        /// <summary>
        /// 执行一个查询语句，返回一个关联的DataReader实例
        /// </summary>
        /// <param name="sql">要执行的查询语句</param>
        /// <param name="commandType">执行的SQL语句的类型</param>
        /// <returns></returns>
        public DbDataReader ExecuteReader(string sql, CommandType commandType = CommandType.Text)
        {
            return ExecuteReader(sql, new List<MyDbParameter>(), commandType);
        }
        /// <summary>     
        /// 执行一个查询语句，返回一个关联的DataReader实例     
        /// </summary>     
        /// <param name="sql">要执行的查询语句</param>     
        /// <param name="parameters">执行SQL查询语句所需要的参数</param>  
        /// <param name="commandType">执行的SQL语句的类型</param>  
        /// <returns></returns>   
        public DbDataReader ExecuteReader(string sql, IList<MyDbParameter> parameters, CommandType commandType = CommandType.Text)
        {
            DbCommand command = DbHelper.CreateDbCommand(providerFactory, ConnectionString, sql, parameters, commandType);
            command.Connection.Open();
            return command.ExecuteReader(CommandBehavior.CloseConnection);
        }

        /// <summary>
        /// 执行一个查询语句，返回一个包含查询结果的DataTable
        /// </summary>
        /// <param name="sql">要执行的查询语句</param>
        /// <returns></returns>
        public DataTable ExecuteDataTable(string sql, CommandType commandType = CommandType.Text)
        {
            return ExecuteDataTable(sql, new List<MyDbParameter>(), commandType);
        }
        /// <summary>     
        /// 执行一个查询语句，返回一个包含查询结果的DataTable     
        /// </summary>     
        /// <param name="sql">要执行的查询语句</param>     
        /// <param name="parameters">执行SQL查询语句所需要的参数</param>  
        /// <param name="commandType">执行的SQL语句的类型</param>  
        /// <returns></returns>  
        public DataTable ExecuteDataTable(string sql, IList<MyDbParameter> parameters, CommandType commandType = CommandType.Text)
        {
            using (DbCommand command = DbHelper.CreateDbCommand(providerFactory, ConnectionString, sql, parameters, commandType))
            {
                using (DbDataAdapter adapter = providerFactory.CreateDataAdapter())
                {
                    adapter.SelectCommand = command;
                    DataTable data = new DataTable();
                    adapter.Fill(data);
                    return data;
                }
            }
        }
        /// <summary>
        /// 执行一组查询语句，返回一个包含查询结果的DataSet
        /// </summary>
        /// <param name="sqlDic">查询语句字典，键为表名，值为查询语句</param>
        /// <returns></returns>
        public DataSet ExecuteDataSet(Dictionary<string, string> sqlDic)
        {
            DataSet result = new DataSet();
            foreach(string key in sqlDic.Keys)
            {
                DataTable tempTable = ExecuteDataTable(sqlDic[key]);
                tempTable.TableName = key;
                result.Tables.Add(tempTable.Copy());
            }
            return result;
        }
        /// <summary>
        /// 批量将DataTable中的数据存到数据库中相应的表内（重复数据先删后插）
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="sourceTable">需要存储的数据</param>
        /// <param name="keyColumns">删除重复数据的条件字段</param>
        /// <param name="msgStr">执行信息</param>
        /// <param name="excludeColumnName">需要排除的字段</param>
        /// <returns></returns>
        public int BulkSave(string tableName, DataTable sourceTable, string[] keyColumns, ref string msgStr, params string[] excludeColumnName)
        {
            int result;
            string[] columns = DbHelper.GetDataTableColumnName(sourceTable, excludeColumnName);

            using (DbConnection conn = providerFactory.CreateConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                DbTransaction transaction = conn.BeginTransaction();
                DbCommand cmd = conn.CreateCommand();
                cmd.Transaction = transaction;

                string sqlMsg = String.Empty;
                try
                {
                    foreach (DataRow dr in sourceTable.Rows)
                    {
                        sqlMsg = String.Empty;
                        List<DbParameter> parameters = new List<DbParameter>();
                        string insertStr = DbHelper.DataRowToInsert(tableName, dr, columns, keyColumns, parameters, ref sqlMsg);
                        DbHelper.ExecuteSQLForTransaction(providerFactory, cmd, insertStr, parameters.ToArray());
                    }
                    transaction.Commit();
                    result = sourceTable.Rows.Count;
                }
                catch
                {
                    msgStr += sqlMsg;
                    transaction.Rollback();
                    result = -1;
                    //throw new Exception(ex.Source + ":" + ex.Message);
                }
            }
            return result;
        }
        /// <summary>
        /// 批量将DataTable中的数据存到数据库中相应的表内（重复数据先删后插）
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="sourceTable">需要存储的数据</param>
        /// <param name="keyColumns">删除重复数据的条件字段</param>
        /// <param name="wrongRow">报错数据行</param>
        /// <param name="msgStr">执行信息</param>
        /// <param name="excludeColumnName">需要排除的字段</param>
        /// <returns></returns>
        public int BulkSave(string tableName, DataTable sourceTable, string[] keyColumns, out DataRow wrongRow, 
            ref string msgStr, params string[] excludeColumnName)
        {
            int result;
            DataRow wrong = null;
            string[] columns = DbHelper.GetDataTableColumnName(sourceTable, excludeColumnName);

            using (DbConnection conn = providerFactory.CreateConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                DbTransaction transaction = conn.BeginTransaction();
                DbCommand cmd = conn.CreateCommand();
                cmd.Transaction = transaction;

                string sqlMsg = String.Empty;
                try
                {
                    foreach (DataRow dr in sourceTable.Rows)
                    {
                        wrong = dr;
                        sqlMsg = String.Empty;
                        List<DbParameter> parameters = new List<DbParameter>();
                        string insertStr = DbHelper.DataRowToInsert(tableName, dr, columns, keyColumns, parameters, ref sqlMsg);
                        DbHelper.ExecuteSQLForTransaction(providerFactory, cmd, insertStr, parameters.ToArray());
                    }
                    transaction.Commit();
                    result = sourceTable.Rows.Count;
                }
                catch
                {
                    msgStr += sqlMsg;
                    transaction.Rollback();
                    result = -1;
                    //throw new Exception(ex.Source + ":" + ex.Message);
                }
            }
            wrongRow = wrong;
            return result;
        }
        /// <summary>
        /// 批量将DataTable中的数据存到数据库中相应的表内（重复数据先删后插）,并返回报错数据
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="sourceTable">需要存储的数据</param>
        /// <param name="keyColumns">删除重复数据的条件字段</param>
        /// <param name="excludeColumnName">需要排除的字段</param>
        /// <returns>报错数据，其中字段WrongSQL记录执行的SQL</returns>
        public DataTable BulkSave(string tableName, DataTable sourceTable, string[] keyColumns, params string[] excludeColumnName)
        {
            DataTable result = sourceTable.Clone();
            result.Columns.Add(new DataColumn("WrongSQL"));
            DataRow wrong = null;
            string[] columns = DbHelper.GetDataTableColumnName(sourceTable, excludeColumnName);

            using (DbConnection conn = providerFactory.CreateConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                //DbTransaction transaction = conn.BeginTransaction();
                DbCommand cmd = conn.CreateCommand();
                //cmd.Transaction = transaction;

                string sqlMsg = String.Empty;
                foreach (DataRow dr in sourceTable.Rows)
                {          
                    wrong = dr;
                    sqlMsg = String.Empty;
                    List<DbParameter> parameters = new List<DbParameter>();
                    string insertStr = DbHelper.DataRowToInsert(tableName, dr, columns, keyColumns, parameters, ref sqlMsg);
                    if (insertStr == null || insertStr == String.Empty)
                    {
                        DataRow wrongRow = result.NewRow();
                        wrongRow.ItemArray = dr.ItemArray;
                        wrongRow["WrongSQL"] = sqlMsg;
                        result.Rows.Add(wrongRow);
                        continue;
                    }
                    try
                    {
                        DbHelper.ExecuteSQLForTransaction(providerFactory, cmd, insertStr, parameters.ToArray());
                    }
                    catch
                    {
                        DataRow wrongRow = result.NewRow();
                        wrongRow.ItemArray = dr.ItemArray;
                        wrongRow["WrongSQL"] = sqlMsg;
                        result.Rows.Add(wrongRow);
                    }
                }
                //transaction.Commit();
            }
            return result;
        }
        /// <summary>
        /// 批量将DataTable中的数据存到数据库中相应的表内
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="sourceTable">需要存储的数据</param>
        /// <param name="excludeColumnName">需要排除的字段</param>
        /// <returns></returns>
        public int BulkSave(string tableName, DataTable sourceTable, ref string msgStr, params string[] excludeColumnName)
        {
            int result;
            string[] columns = DbHelper.GetDataTableColumnName(sourceTable, excludeColumnName);

            using (DbConnection conn = providerFactory.CreateConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                DbTransaction transaction = conn.BeginTransaction();
                DbCommand cmd = conn.CreateCommand();
                cmd.Transaction = transaction;

                string sqlMsg = String.Empty;
                try
                {
                    foreach (DataRow dr in sourceTable.Rows)
                    {
                        sqlMsg = String.Empty;
                        List<DbParameter> parameters = new List<DbParameter>();
                        string insertStr = DbHelper.DataRowToInsert(tableName, dr, columns, parameters, ref sqlMsg);
                        DbHelper.ExecuteSQLForTransaction(providerFactory, cmd, insertStr, parameters.ToArray());
                    }
                    transaction.Commit();
                    result = sourceTable.Rows.Count;
                }
                catch
                {
                    msgStr += sqlMsg;
                    transaction.Rollback();
                    result = -1;
                    //throw new Exception(ex.Source + ":" + ex.Message);
                }
            }
            return result;
        }
        /// <summary>
        /// 批量更新指定数据库表中的数据
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="sourceTable">数据源</param>
        /// <param name="keyColumnName">定位字段名</param>
        /// <returns></returns>
        public int BulkUpdate(string tableName, DataTable sourceTable, string[] keyColumnName)
        {
            int result;
            string[] columns = DbHelper.GetDataTableColumnName(sourceTable, keyColumnName);

            using (DbConnection conn = providerFactory.CreateConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                DbTransaction transaction = conn.BeginTransaction();
                DbCommand cmd = conn.CreateCommand();
                cmd.Transaction = transaction;

                try
                {
                    foreach (DataRow dr in sourceTable.Rows)
                    {
                        List<DbParameter> parameters = new List<DbParameter>();
                        string updateStr = DbHelper.DataRowToUpdate(tableName, dr, columns, keyColumnName, parameters);
                        DbHelper.ExecuteSQLForTransaction(providerFactory, cmd, updateStr, parameters.ToArray());
                    }
                    transaction.Commit();
                    result = sourceTable.Rows.Count;
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    result = -1;
                    throw new Exception(ex.Source + ":" + ex.Message);
                }

            }
            return result;
        }
    }
}
