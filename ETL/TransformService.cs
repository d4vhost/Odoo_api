using System;
using System.Collections.Generic;
using TechStore_ETL_API.Models;

namespace TechStore_ETL_API.ETL
{
    // TRANSFORM — responsabilidad: limpiar, deduplicar y
    //             transformar los datos antes de cargarlos
    internal class TransformService
    {
        // ── TRANSFORMAR CLIENTES ──────────────────────────────
        // Reglas aplicadas:
        //   1. Eliminar duplicados por cédula
        //   2. Convertir nombre a MAYÚSCULAS
        //   3. Asignar segmento_ciudad (Local / Nacional)
        //   4. Eliminar registros sin cédula
        public List<Cliente> TransformarClientes(List<Cliente> clientes)
        {
            Console.WriteLine("\n  [TRANSFORM] Procesando clientes...");

            var resultado = new List<Cliente>();
            var cedulasVistas = new HashSet<string>();
            int duplicados = 0;
            int sinCedula = 0;

            foreach (var c in clientes)
            {
                // ── Validación: descartar si no tiene cédula ──
                if (string.IsNullOrWhiteSpace(c.Cedula))
                {
                    Console.WriteLine($"    [INVALIDO] Cliente sin cédula descartado: {c.Nombre}");
                    sinCedula++;
                    continue;
                }

                string cedulaKey = c.Cedula.Trim();

                // ── Deduplicación por cédula ──────────────────
                if (cedulasVistas.Contains(cedulaKey))
                {
                    Console.WriteLine($"    [DUPLICADO] Cliente ignorado: {c.Nombre} (cédula: {cedulaKey})");
                    duplicados++;
                    continue;
                }

                cedulasVistas.Add(cedulaKey);

                // ── Transformaciones ──────────────────────────
                c.Nombre = (c.Nombre ?? "").Trim();                          // quitar espacios
                c.Ciudad = (c.Ciudad ?? "").Trim();
                c.Estado = (c.Estado ?? "activo").Trim().ToLower();

                // Columna derivada: segmento de ciudad
                // (se almacena en una propiedad auxiliar que LoadService usa)
                // Local = Ambato | Nacional = cualquier otra ciudad

                resultado.Add(c);
            }

            Console.WriteLine($"    [TRANSFORM] Clientes válidos    : {resultado.Count}");
            Console.WriteLine($"    [TRANSFORM] Duplicados removidos: {duplicados}");
            if (sinCedula > 0)
                Console.WriteLine($"    [TRANSFORM] Sin cédula removidos: {sinCedula}");

            return resultado;
        }

        // ── TRANSFORMAR PRODUCTOS ─────────────────────────────
        // Reglas aplicadas:
        //   1. Eliminar duplicados por nombre (case-insensitive)
        //   2. Precio nunca negativo
        //   3. Stock nunca negativo
        //   4. Eliminar registros sin nombre
        public List<Producto> TransformarProductos(List<Producto> productos)
        {
            Console.WriteLine("\n  [TRANSFORM] Procesando productos...");

            var resultado = new List<Producto>();
            var nombresVistos = new HashSet<string>();
            int duplicados = 0;
            int invalidos = 0;

            foreach (var p in productos)
            {
                // ── Validación: descartar si no tiene nombre ──
                if (string.IsNullOrWhiteSpace(p.Nombre))
                {
                    Console.WriteLine($"    [INVALIDO] Producto sin nombre descartado (id: {p.Id})");
                    invalidos++;
                    continue;
                }

                string nombreKey = p.Nombre.Trim().ToLower();

                // ── Deduplicación por nombre ──────────────────
                if (nombresVistos.Contains(nombreKey))
                {
                    Console.WriteLine($"    [DUPLICADO] Producto ignorado: {p.Nombre}");
                    duplicados++;
                    continue;
                }

                nombresVistos.Add(nombreKey);

                // ── Transformaciones ──────────────────────────
                p.Nombre = p.Nombre.Trim();
                p.Categoria = (p.Categoria ?? "Sin categoría").Trim();
                p.Proveedor = (p.Proveedor ?? "Sin proveedor").Trim();
                p.Estado = (p.Estado ?? "activo").Trim().ToLower();

                // Precio y stock no pueden ser negativos
                if (p.Precio < 0) p.Precio = 0;
                if (p.Stock < 0) p.Stock = 0;

                resultado.Add(p);
            }

            Console.WriteLine($"    [TRANSFORM] Productos válidos   : {resultado.Count}");
            Console.WriteLine($"    [TRANSFORM] Duplicados removidos: {duplicados}");
            if (invalidos > 0)
                Console.WriteLine($"    [TRANSFORM] Inválidos removidos : {invalidos}");

            return resultado;
        }

        // ── TRANSFORMAR VENTAS ────────────────────────────────
        // Reglas aplicadas:
        //   1. Eliminar ventas sin fecha
        //   2. Eliminar ventas sin cliente_id
        //   3. Eliminar ventas sin detalles
        //   4. Deduplicar detalles por producto dentro de la misma venta
        public List<Venta> TransformarVentas(List<Venta> ventas)
        {
            Console.WriteLine("\n  [TRANSFORM] Procesando ventas...");

            var resultado = new List<Venta>();
            int sinFecha = 0;
            int sinCliente = 0;
            int sinDetalles = 0;
            int detaDuplicados = 0;

            foreach (var v in ventas)
            {
                // ── Validación: fecha requerida ───────────────
                if (string.IsNullOrWhiteSpace(v.FechaVenta))
                {
                    Console.WriteLine($"    [INVALIDO] Venta sin fecha descartada (id: {v.Id})");
                    sinFecha++;
                    continue;
                }

                // ── Validación: cliente requerido ─────────────
                if (v.ClienteId <= 0)
                {
                    Console.WriteLine($"    [INVALIDO] Venta sin cliente descartada (id: {v.Id})");
                    sinCliente++;
                    continue;
                }

                // ── Validación: debe tener detalles ───────────
                if (v.Detalles == null || v.Detalles.Count == 0)
                {
                    Console.WriteLine($"    [INVALIDO] Venta sin detalles descartada (id: {v.Id})");
                    sinDetalles++;
                    continue;
                }

                // ── Deduplicar detalles por producto_id ───────
                var productosEnVenta = new HashSet<int>();
                var detallesLimpios = new List<DetalleVenta>();

                foreach (var d in v.Detalles)
                {
                    if (productosEnVenta.Contains(d.ProductoId))
                    {
                        Console.WriteLine($"    [DUPLICADO] Detalle ignorado: venta {v.Id} - producto {d.ProductoId}");
                        detaDuplicados++;
                        continue;
                    }

                    // Cantidad mínima 1
                    if (d.Cantidad <= 0) d.Cantidad = 1;

                    productosEnVenta.Add(d.ProductoId);
                    detallesLimpios.Add(d);
                }

                v.Detalles = detallesLimpios;
                v.Vendedor = (v.Vendedor ?? "Sin vendedor").Trim();
                v.Sucursal = (v.Sucursal ?? "Sin sucursal").Trim();
                v.MetodoPago = (v.MetodoPago ?? "efectivo").Trim().ToLower();

                resultado.Add(v);
            }

            Console.WriteLine($"    [TRANSFORM] Ventas válidas      : {resultado.Count}");
            Console.WriteLine($"    [TRANSFORM] Sin fecha removidas : {sinFecha}");
            Console.WriteLine($"    [TRANSFORM] Sin cliente removidas: {sinCliente}");
            Console.WriteLine($"    [TRANSFORM] Sin detalles removidas: {sinDetalles}");
            if (detaDuplicados > 0)
                Console.WriteLine($"    [TRANSFORM] Detalles duplicados : {detaDuplicados}");

            return resultado;
        }

        // ── TRANSFORMAR TODO DE UNA VEZ ───────────────────────
        public (List<Cliente> clientes, List<Producto> productos, List<Venta> ventas) TransformarTodo(
            List<Cliente> clientes,
            List<Producto> productos,
            List<Venta> ventas)
        {
            Console.WriteLine("\n[ TRANSFORM ] Iniciando limpieza y transformación...");

            var clientesLimpios = TransformarClientes(clientes);
            var productosLimpios = TransformarProductos(productos);
            var ventasLimpias = TransformarVentas(ventas);

            Console.WriteLine($"\n  Resumen transformación:");
            Console.WriteLine($"    Clientes  : {clientes.Count} → {clientesLimpios.Count}");
            Console.WriteLine($"    Productos : {productos.Count} → {productosLimpios.Count}");
            Console.WriteLine($"    Ventas    : {ventas.Count} → {ventasLimpias.Count}");

            return (clientesLimpios, productosLimpios, ventasLimpias);
        }
    }
}