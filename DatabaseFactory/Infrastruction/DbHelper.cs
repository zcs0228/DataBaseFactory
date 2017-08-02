using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseFactory
{
    public class DbHelper
    {
        public static string[] GetDataTableColumnName(DataTable dt, string[] excludeColumn)
        {
            IList<string> result = new List<string>();

            foreach (DataColumn item in dt.Columns)
            {
                if (!excludeColumn.Contains(item.ColumnName))
                {
                    result.Add(item.ColumnName);
                }
            }
            return result.ToArray();
        }

        /// <summary>
        /// 将DataRow转换为Insert语句
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="dr">数据源</param>
        /// <param name="columns">列名</param>
        /// <param name="parameters">参数集合</param>
        /// <param name="msgStr">错误信息</param>
        /// <returns></returns>
        public static string DataRowToInsert(string tableName, DataRow dr, string[] columns, List<DbParameter> parameters, ref string msgStr)
        {
            string baseString = "INSERT INTO ";
            StringBuilder insertStr = new StringBuilder();
            StringBuilder insertMsgStr = new StringBuilder();
            insertStr.Append(baseString).Append(tableName);
            insertMsgStr.Append(insertStr.ToString());

            StringBuilder columnStr = new StringBuilder("(");
            StringBuilder valueStr = new StringBuilder("(");
            StringBuilder valueMsgStr = new StringBuilder("(");
            foreach (string item in columns)
            {
                string guid = Guid.NewGuid().ToString().Replace("-", "");

                columnStr.Append(item).Append(",");
                valueStr.Append("@I").Append(guid).Append(",");
                valueMsgStr.Append("'").Append(dr[item].ToString()).Append("',");
                parameters.Add(new MyDbParameter("@I" + guid, dr[item]));
            }
            columnStr.Remove(columnStr.Length - 1, 1).Append(")");
            valueStr.Remove(valueStr.Length - 1, 1).Append(")");
            valueMsgStr.Remove(valueMsgStr.Length - 1, 1).Append(")");
            insertStr.Append(" ").Append(columnStr.ToString());
            insertStr.Append(" VALUES ").Append(valueStr.ToString()).Append(";");
            insertMsgStr.Append(" ").Append(columnStr.ToString());
            insertMsgStr.Append(" VALUES ").Append(valueMsgStr.ToString()).Append(";");

            msgStr += insertMsgStr.ToString();
            return insertStr.ToString();
        }
        /// <summary>
        /// 将DataRow转换为Insert语句(先删后插)
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="dr">数据源</param>
        /// <param name="columns">列名</param>
        /// <param name="keyColumns">删除语句的条件字段</param>
        /// <param name="parameters">参数集合</param>
        /// <param name="msgStr">错误信息</param>
        /// <returns></returns>
        public static string DataRowToInsert(string tableName, DataRow dr, string[] columns, string[] keyColumns, List<DbParameter> parameters, ref string msgStr)
        {
            string result = String.Empty;

            StringBuilder deleteStr = new StringBuilder();//带参数的执行SQL
            StringBuilder deleteMsgStr = new StringBuilder();//不带参数的直接SQL
            if (keyColumns.Length > 0)
            {
                string baseDeleteString = "DELETE FROM ";
                deleteStr.Append(baseDeleteString).Append(tableName).Append(" WHERE ");
                deleteMsgStr.Append(deleteStr.ToString());

                foreach (string item in keyColumns)
                {
                    string guid = Guid.NewGuid().ToString().Replace("-", "");
                    deleteStr.Append(item).Append("=").Append("@D").Append(guid).Append(" AND ");
                    parameters.Add(new MyDbParameter("@D" + guid, dr[item]));
                    deleteMsgStr.Append(item).Append("='").Append(dr[item]).Append("' AND ");
                }
                deleteStr.Remove(deleteStr.Length - 5, 5);
                deleteMsgStr.Remove(deleteMsgStr.Length - 5, 5);
                deleteStr.Append(";");
                deleteMsgStr.Append(";");
            }


            string baseString = "INSERT INTO ";         
            StringBuilder insertStr = new StringBuilder();
            StringBuilder insertMsgStr = new StringBuilder();         
            insertStr.Append(baseString).Append(tableName);
            insertMsgStr.Append(insertStr.ToString());

            StringBuilder columnStr = new StringBuilder("(");
            StringBuilder valueStr = new StringBuilder("(");
            StringBuilder valueMsgStr = new StringBuilder("(");
            foreach (string item in columns)
            {
                string guid = Guid.NewGuid().ToString().Replace("-", "");

                columnStr.Append(item).Append(",");
                valueStr.Append("@I").Append(guid).Append(",");
                valueMsgStr.Append("'").Append(dr[item].ToString()).Append("',");
                parameters.Add(new MyDbParameter("@I" + guid, dr[item]));
            }
            columnStr.Remove(columnStr.Length - 1, 1).Append(")");
            valueStr.Remove(valueStr.Length - 1, 1).Append(")");
            valueMsgStr.Remove(valueMsgStr.Length - 1, 1).Append(")");
            insertStr.Append(" ").Append(columnStr.ToString());
            insertStr.Append(" VALUES ").Append(valueStr.ToString()).Append(";");
            insertMsgStr.Append(" ").Append(columnStr.ToString());
            insertMsgStr.Append(" VALUES ").Append(valueMsgStr.ToString()).Append(";");

            if (deleteMsgStr.Length > 0)
            {
                msgStr += deleteMsgStr.ToString() + "\n";
            }
            msgStr += insertMsgStr.ToString();
            if (deleteStr.Length > 0)
            {
                result += deleteStr.ToString() + "\n";
            }
            result += insertStr.ToString();
            return result;
        }

        /// <summary>
        /// 将DataRow转换为Update语句
        /// </summary>
        /// <param name="tableName">数据库表名</param>
        /// <param name="dr">数据源</param>
        /// <param name="columns">列名</param>
        /// <param name="keyColumnName">判断条件列名</param>
        /// <param name="parameters">参数集合</param>
        /// <returns></returns>
        public static string DataRowToUpdate(string tableName, DataRow dr, string[] columns,
            string[] keyColumnName, List<DbParameter> parameters)
        {
            string baseString = "UPDATE ";
            StringBuilder updateStr = new StringBuilder();
            updateStr.Append(baseString).Append(tableName).Append(" SET ");
            foreach (string item in columns)
            {
                string guid = Guid.NewGuid().ToString().Replace("-", "");

                updateStr.Append(item).Append("=@U").Append(guid).Append(",");
                parameters.Add(new MyDbParameter("@U" + guid, dr[item]));
            }
            updateStr.Remove(updateStr.Length - 1, 1);
            updateStr.Append(" WHERE ");
            foreach (string item in keyColumnName)
            {
                string guid = Guid.NewGuid().ToString().Replace("-", "");

                updateStr.Append(item).Append("=@W").Append(guid).Append(" AND ");
                parameters.Add(new MyDbParameter("@W" + guid, dr[item]));
            }
            updateStr.Remove(updateStr.Length - 5, 5);
            return updateStr.ToString();
        }

        /// <summary>
        /// 执行SQL语句
        /// </summary>
        /// <param name="cmd"></param>
        /// <param name="sqlString"></param>
        /// <param name="parameters"></param>
        public static void ExecuteSQLForTransaction(DbProviderFactory providerFactory, DbCommand cmd, 
            string sqlString, params DbParameter[] parameters)
        {
            cmd.CommandText = sqlString;
            cmd.Parameters.Clear();
            foreach (var item in parameters)
            {
                DbParameter temp = providerFactory.CreateParameter();
                temp.ParameterName = item.ParameterName;
                temp.Value = item.Value;
                temp.Size = item.Size;
                temp.DbType = item.DbType;
                temp.Direction = item.Direction;
                #region .net framework 4.5.1及以后版本
                //temp.Precision = item.Precision;
                //temp.Scale = item.Scale;
                #endregion
                cmd.Parameters.Add(temp);
            }
            cmd.ExecuteNonQuery();
        }

        /// <summary>  
        /// 创建一个DbCommand对象  
        /// </summary>
        /// <param name="providerFactory">数据库访问提供者工厂类</param>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="sql">要执行的查询语句</param>     
        /// <param name="parameters">执行SQL查询语句所需要的参数</param>  
        /// <param name="commandType">执行的SQL语句的类型</param>  
        /// <returns></returns>  
        public static DbCommand CreateDbCommand(DbProviderFactory providerFactory, string connectionString, 
            string sql, IList<MyDbParameter> parameters, CommandType commandType)
        {
            DbConnection connection = providerFactory.CreateConnection();
            DbCommand command = providerFactory.CreateCommand();
            connection.ConnectionString = connectionString;
            command.CommandText = sql;
            command.CommandType = commandType;
            command.Connection = connection;
            if (!(parameters == null || parameters.Count == 0))
            {
                foreach (DbParameter parameter in parameters)
                {
                    DbParameter temp = providerFactory.CreateParameter();
                    temp.ParameterName = parameter.ParameterName;
                    temp.Value = parameter.Value;
                    temp.Size = parameter.Size;
                    temp.DbType = parameter.DbType;
                    temp.Direction = parameter.Direction;
                    #region .net framework 4.5.1及以后版本
                    //temp.Precision = parameter.Precision;
                    //temp.Scale = parameter.Scale;
                    #endregion
                    command.Parameters.Add(temp);
                }
            }
            return command;
        }
    }
}
