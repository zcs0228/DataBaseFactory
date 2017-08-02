using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;

namespace DatabaseFactory.Infrastruction
{
    public static class MyDbProviderFactories
    {
        public static DbProviderFactory GetFactory(DbProviderInvariant providerInvariant)
        {
            string path = AppDomain.CurrentDomain.BaseDirectory + providerInvariant.DllName;
            Assembly assembly = Assembly.LoadFrom(path);
            if (assembly == null)
            {
                throw new Exception("无法加载程序集" + providerInvariant.DllName + "！");
            }
            Type t = assembly.GetType(providerInvariant.FullClassName, true);
            if (t == null)
            {
                throw new Exception("无法反射加载类型：" + providerInvariant.FullClassName + "！");
            }
            Object o = Activator.CreateInstance(t);
            return o as DbProviderFactory;
        }
    }
}
