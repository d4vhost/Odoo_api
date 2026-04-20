using System;
using System.Collections.Generic;
using TechStore_ETL_API.Models;

namespace TechStore_ETL_API.ETL
{
    // ══════════════════════════════════════════════════════════
    // EXTRACT — responsabilidad: llamar a las APIs de Odoo
    //           y devolver los datos crudos
    // ══════════════════════════════════════════════════════════
    internal class ExtractService
    {
        private readonly ApiClient _api;

        public ExtractService()
        {
            _api = new ApiClient();
        }

        // ── EXTRAER CLIENTES ──────────────────────────────────
        public List<Cliente> ExtraerClientes()
        {
            Console.WriteLine("  [EXTRACT] Conectando a API /api/clientes...");
            try
            {
                var clientes = _api.GetClientes();
                Console.WriteLine($"  [EXTRACT] {clientes.Count} clientes recibidos desde Odoo ✓");
                return clientes;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [EXTRACT][ERROR] Clientes: {ex.Message}");
                return new List<Cliente>();
            }
        }

        // ── EXTRAER PRODUCTOS ─────────────────────────────────
        public List<Producto> ExtraerProductos()
        {
            Console.WriteLine("  [EXTRACT] Conectando a API /api/productos...");
            try
            {
                var productos = _api.GetProductos();
                Console.WriteLine($"  [EXTRACT] {productos.Count} productos recibidos desde Odoo ✓");
                return productos;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [EXTRACT][ERROR] Productos: {ex.Message}");
                return new List<Producto>();
            }
        }

        // ── EXTRAER VENTAS ────────────────────────────────────
        public List<Venta> ExtraerVentas()
        {
            Console.WriteLine("  [EXTRACT] Conectando a API /api/ventas...");
            try
            {
                var ventas = _api.GetVentas();
                Console.WriteLine($"  [EXTRACT] {ventas.Count} ventas recibidas desde Odoo ✓");
                return ventas;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [EXTRACT][ERROR] Ventas: {ex.Message}");
                return new List<Venta>();
            }
        }

        // ── EXTRAER TODO DE UNA VEZ ───────────────────────────
        public (List<Cliente> clientes, List<Producto> productos, List<Venta> ventas) ExtraerTodo()
        {
            Console.WriteLine("\n[ EXTRACT ] Iniciando extracción desde APIs de Odoo...");
            var clientes = ExtraerClientes();
            var productos = ExtraerProductos();
            var ventas = ExtraerVentas();

            Console.WriteLine($"\n  Resumen extracción:");
            Console.WriteLine($"    Clientes  : {clientes.Count}");
            Console.WriteLine($"    Productos : {productos.Count}");
            Console.WriteLine($"    Ventas    : {ventas.Count}");

            return (clientes, productos, ventas);
        }
    }
}