using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TechStore_ETL_API
{
    public static class Config
    {
        public const string OdooBaseUrl = "http://localhost:8069";
        public const string SqlServer =
            "Server=10.79.17.222,1433;" +
            "Database=techstore_bi_p;" +
            "User Id=sa;Password=admin;" +
            "TrustServerCertificate=True;";
    }
}
