using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TechStore_ETL_API.Models
{
    public class DetalleVenta
    {
        [JsonProperty("id")] public int Id { get; set; }
        [JsonProperty("producto_id")] public int ProductoId { get; set; }
        [JsonProperty("producto_nombre")] public string ProductoNombre { get; set; }
        [JsonProperty("cantidad")] public int Cantidad { get; set; }
        [JsonProperty("precio_unitario")] public double PrecioUnitario { get; set; }
        [JsonProperty("subtotal")] public double Subtotal { get; set; }
    }

    public class Venta
    {
        [JsonProperty("id")] public int Id { get; set; }
        [JsonProperty("codigo")] public string Codigo { get; set; }
        [JsonProperty("fecha_venta")] public string FechaVenta { get; set; }
        [JsonProperty("cliente_id")] public int ClienteId { get; set; }
        [JsonProperty("cliente_nombre")] public string ClienteNombre { get; set; }
        [JsonProperty("metodo_pago")] public string MetodoPago { get; set; }
        [JsonProperty("vendedor")] public string Vendedor { get; set; }
        [JsonProperty("sucursal")] public string Sucursal { get; set; }
        [JsonProperty("subtotal")] public double Subtotal { get; set; }
        [JsonProperty("iva")] public double Iva { get; set; }
        [JsonProperty("total")] public double Total { get; set; }
        [JsonProperty("detalles")] public List<DetalleVenta> Detalles { get; set; }
    }
}
