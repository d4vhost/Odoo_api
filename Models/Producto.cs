using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TechStore_ETL_API.Models
{
    public class Producto
    {
        [JsonProperty("id")] public int Id { get; set; }
        [JsonProperty("nombre")] public string Nombre { get; set; }
        [JsonProperty("categoria")] public string Categoria { get; set; }
        [JsonProperty("precio")] public double Precio { get; set; }
        [JsonProperty("stock")] public int Stock { get; set; }
        [JsonProperty("proveedor")] public string Proveedor { get; set; }
        [JsonProperty("fecha_ingreso")] public string FechaIngreso { get; set; }
        [JsonProperty("estado")] public string Estado { get; set; }
    }
}
