using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Dynamic;
using System.Data.SqlClient;
using System.Data.Common;
using DatabaseFactory.Infrastruction;

namespace DatabaseFactory
{
    public struct DbProviderInvariant
    {
        public string DllName;
        public string FullClassName;
    }
    public class ProviderFactory
    {
        //private static Dictionary<DbProviderType, string> providerInvariantNames = new Dictionary<DbProviderType, string>();
        private static Dictionary<DbProviderType, DbProviderInvariant> providerInvariant = new Dictionary<DbProviderType, DbProviderInvariant>();
        private static Dictionary<DbProviderType, DbProviderFactory> providerFactoies = new Dictionary<DbProviderType, DbProviderFactory>(20);
        static ProviderFactory()
        {
            //加载已知的数据库访问类的程序集  
            //providerInvariantNames.Add(DbProviderType.MSSQL, "System.Data.SqlClient");
            //providerInvariantNames.Add(DbProviderType.Oracle, "Oracle.DataAccess.Client");//Oracle.DataAccess.Client为Factory所在的命名空间
            //providerInvariantNames.Add(DbProviderType.MySql, "MySql.Data.MySqlClient");

            DbProviderInvariant MSSQLProvider = new DbProviderInvariant
            {
                DllName = "System.Data.dll",
                FullClassName = "System.Data.SqlClient.SqlClientFactory"
            };
            providerInvariant.Add(DbProviderType.MSSQL, MSSQLProvider);
            DbProviderInvariant OracleProvider = new DbProviderInvariant
            {
                DllName = "Oracle.DataAccess.dll",
                FullClassName = "Oracle.DataAccess.Client.OracleClientFactory"
            };
            providerInvariant.Add(DbProviderType.Oracle, OracleProvider);
            DbProviderInvariant MySqlProvider = new DbProviderInvariant
            {
                DllName = "MySql.Data.dll",
                FullClassName = "MySql.Data.MySqlClient.MySqlClientFactory"
            };
            providerInvariant.Add(DbProviderType.MySql, MySqlProvider);
        }
        /// <summary>  
        /// 获取指定数据库类型对应的程序集名称  
        /// </summary>  
        /// <param name="providerType">数据库类型枚举</param>  
        /// <returns></returns>  
        //public static string GetProviderInvariantName(DbProviderType providerType)
        //{
        //    return providerInvariantNames[providerType];
        //}

        /// <summary>  
        /// 获取指定类型的数据库对应的DbProviderFactory  
        /// </summary>  
        /// <param name="providerType">数据库类型枚举</param>  
        /// <returns></returns>  
        public static DbProviderFactory GetDbProviderFactory(DbProviderType providerType)
        {
            //如果还没有加载，则加载该DbProviderFactory  
            if (!providerFactoies.ContainsKey(providerType))
            {
                providerFactoies.Add(providerType, ImportDbProviderFactory(providerType));
            }
            return providerFactoies[providerType];
        }
        /// <summary>  
        /// 加载指定数据库类型的DbProviderFactory  
        /// </summary>  
        /// <param name="providerType">数据库类型枚举</param>  
        /// <returns></returns>  
        private static DbProviderFactory ImportDbProviderFactory(DbProviderType providerType)
        {
            //string providerName = providerInvariantNames[providerType];
            DbProviderInvariant DbProvider = providerInvariant[providerType];
            DbProviderFactory factory = null;
            try
            {
                if (providerType == DbProviderType.MSSQL)
                {
                    factory = SqlClientFactory.Instance;
                }
                else
                {
                    //从全局程序集中查找  
                    //factory = DbProviderFactories.GetFactory(providerName);//通过配置文件中DbProviderFactories配置获取Factory
                    factory = MyDbProviderFactories.GetFactory(DbProvider);//自定义反射获取Factory
                }
            }
            catch(Exception ex)
            {
                factory = null;
            }
            return factory;
        }
    }
}
