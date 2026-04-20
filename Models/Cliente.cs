using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TechStore_ETL_API.Models
{
    public class Cliente
    {
        [JsonProperty("id")] public int Id { get; set; }
        [JsonProperty("nombre")] public string Nombre { get; set; }
        [JsonProperty("cedula")] public string Cedula { get; set; }
        [JsonProperty("ciudad")] public string Ciudad { get; set; }
        [JsonProperty("telefono")] public string Telefono { get; set; }
        [JsonProperty("correo")] public string Correo { get; set; }
        [JsonProperty("fecha_registro")] public string FechaRegistro { get; set; }
        [JsonProperty("estado")] public string Estado { get; set; }
    }
}
