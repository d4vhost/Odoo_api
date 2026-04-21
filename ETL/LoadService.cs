using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using TechStore_ETL_API;
using TechStore_ETL_API.Models;

public class LoadService
{
    private readonly string _connStr = Config.SqlServer;

    // ── TRUNCATE ──────────────────────────────────────────────
    public void TruncateAll()
    {
        Console.WriteLine("\n  Limpiando tablas destino...");
        string[] tablas = {
            "fact_ventas_ssis",
            "dim_cliente_ssis",
            "dim_producto_ssis",
            "dim_fecha_ssis",
            "dim_sucursal_ssis",
            "dim_metodo_pago_ssis"
        };
        using (var conn = new SqlConnection(_connStr))
        {
            conn.Open();
            foreach (var tabla in tablas)
            {
                new SqlCommand($"TRUNCATE TABLE dbo.{tabla}", conn).ExecuteNonQuery();
                Console.WriteLine($"    TRUNCATE {tabla} ✓");
            }
        }
    }

    // ── DIM CLIENTE ───────────────────────────────────────────
    public Dictionary<int, int> CargarDimCliente(List<Cliente> clientes)
    {
        Console.WriteLine("\n  Cargando dim_cliente_ssis...");
        var mapa = new Dictionary<int, int>();

        var dt = new DataTable();
        dt.Columns.Add("id_cliente_odoo", typeof(int));
        dt.Columns.Add("nombre", typeof(string));
        dt.Columns.Add("nombre_mayusculas", typeof(string));
        dt.Columns.Add("cedula", typeof(string));
        dt.Columns.Add("ciudad", typeof(string));
        dt.Columns.Add("segmento_ciudad", typeof(string));
        dt.Columns.Add("telefono", typeof(string));
        dt.Columns.Add("correo", typeof(string));
        dt.Columns.Add("estado", typeof(string));

        var cedulasVistas = new HashSet<string>();
        int duplicados = 0;

        foreach (var c in clientes)
        {
            string cedulaKey = (c.Cedula ?? "").Trim();

            if (cedulasVistas.Contains(cedulaKey))
            {
                Console.WriteLine($"    [DUPLICADO] Cliente ignorado: {c.Nombre} (cédula: {cedulaKey})");
                duplicados++;
                continue;
            }

            cedulasVistas.Add(cedulaKey);

            string mayusculas = (c.Nombre ?? "").ToUpper();
            string segmento = c.Ciudad == "Ambato" ? "Local" : "Nacional";

            dt.Rows.Add(
                c.Id, c.Nombre, mayusculas,
                c.Cedula, c.Ciudad, segmento,
                c.Telefono, c.Correo, c.Estado
            );
        }

        if (duplicados > 0)
            Console.WriteLine($"    [LIMPIEZA] {duplicados} clientes duplicados eliminados");

        using (var conn = new SqlConnection(_connStr))
        {
            conn.Open();
            using (var bulk = new SqlBulkCopy(conn))
            {
                bulk.DestinationTableName = "dbo.dim_cliente_ssis";
                bulk.ColumnMappings.Add("id_cliente_odoo", "id_cliente_odoo");
                bulk.ColumnMappings.Add("nombre", "nombre");
                bulk.ColumnMappings.Add("nombre_mayusculas", "nombre_mayusculas");
                bulk.ColumnMappings.Add("cedula", "cedula");
                bulk.ColumnMappings.Add("ciudad", "ciudad");
                bulk.ColumnMappings.Add("segmento_ciudad", "segmento_ciudad");
                bulk.ColumnMappings.Add("telefono", "telefono");
                bulk.ColumnMappings.Add("correo", "correo");
                bulk.ColumnMappings.Add("estado", "estado");
                bulk.WriteToServer(dt);
            }

            var cmd = new SqlCommand(
                "SELECT id_cliente_sk, id_cliente_odoo FROM dbo.dim_cliente_ssis", conn);
            using (var reader = cmd.ExecuteReader())
                while (reader.Read())
                    mapa[(int)reader["id_cliente_odoo"]] = (int)reader["id_cliente_sk"];
        }

        Console.WriteLine($"    → {dt.Rows.Count} filas cargadas ✓");
        return mapa;
    }

    // ── DIM PRODUCTO ──────────────────────────────────────────
    public Dictionary<int, int> CargarDimProducto(List<Producto> productos)
    {
        Console.WriteLine("\n  Cargando dim_producto_ssis...");
        var mapa = new Dictionary<int, int>();

        var dt = new DataTable();
        dt.Columns.Add("id_producto_odoo", typeof(int));
        dt.Columns.Add("nombre", typeof(string));
        dt.Columns.Add("categoria", typeof(string));
        dt.Columns.Add("proveedor", typeof(string));
        dt.Columns.Add("precio", typeof(double));
        dt.Columns.Add("estado", typeof(string));

        var nombresVistos = new HashSet<string>();
        int duplicados = 0;

        foreach (var p in productos)
        {
            string nombreKey = (p.Nombre ?? "").Trim().ToLower();

            if (nombresVistos.Contains(nombreKey))
            {
                Console.WriteLine($"    [DUPLICADO] Producto ignorado: {p.Nombre}");
                duplicados++;
                continue;
            }

            nombresVistos.Add(nombreKey);
            dt.Rows.Add(p.Id, p.Nombre, p.Categoria, p.Proveedor, p.Precio, p.Estado);
        }

        if (duplicados > 0)
            Console.WriteLine($"    [LIMPIEZA] {duplicados} productos duplicados eliminados");

        using (var conn = new SqlConnection(_connStr))
        {
            conn.Open();
            using (var bulk = new SqlBulkCopy(conn))
            {
                bulk.DestinationTableName = "dbo.dim_producto_ssis";
                bulk.ColumnMappings.Add("id_producto_odoo", "id_producto_odoo");
                bulk.ColumnMappings.Add("nombre", "nombre");
                bulk.ColumnMappings.Add("categoria", "categoria");
                bulk.ColumnMappings.Add("proveedor", "proveedor");
                bulk.ColumnMappings.Add("precio", "precio");
                bulk.ColumnMappings.Add("estado", "estado");
                bulk.WriteToServer(dt);
            }

            var cmd = new SqlCommand(
                "SELECT id_producto_sk, id_producto_odoo FROM dbo.dim_producto_ssis", conn);
            using (var reader = cmd.ExecuteReader())
                while (reader.Read())
                    mapa[(int)reader["id_producto_odoo"]] = (int)reader["id_producto_sk"];
        }

        Console.WriteLine($"    → {dt.Rows.Count} filas cargadas ✓");
        return mapa;
    }

    // ── DIM FECHA ─────────────────────────────────────────────
    public void CargarDimFecha(List<Venta> ventas)
    {
        Console.WriteLine("\n  Cargando dim_fecha_ssis...");
        var fechasVistas = new HashSet<int>();

        var dt = new DataTable();
        dt.Columns.Add("id_fecha", typeof(int));
        dt.Columns.Add("fecha", typeof(DateTime));
        dt.Columns.Add("anio", typeof(int));
        dt.Columns.Add("mes", typeof(int));
        dt.Columns.Add("dia", typeof(int));
        dt.Columns.Add("trimestre", typeof(int));
        dt.Columns.Add("nombre_mes", typeof(string));
        dt.Columns.Add("nombre_dia", typeof(string));

        foreach (var v in ventas)
        {
            if (string.IsNullOrEmpty(v.FechaVenta)) continue;
            var fecha = DateTime.Parse(v.FechaVenta);
            int idFecha = int.Parse(fecha.ToString("yyyyMMdd"));

            if (fechasVistas.Contains(idFecha)) continue;
            fechasVistas.Add(idFecha);

            dt.Rows.Add(
                idFecha, fecha.Date,
                fecha.Year, fecha.Month, fecha.Day,
                ((fecha.Month - 1) / 3) + 1,
                fecha.ToString("MMMM"),
                fecha.DayOfWeek.ToString()
            );
        }

        using (var conn = new SqlConnection(_connStr))
        {
            conn.Open();
            using (var bulk = new SqlBulkCopy(conn))
            {
                bulk.DestinationTableName = "dbo.dim_fecha_ssis";
                bulk.ColumnMappings.Add("id_fecha", "id_fecha");
                bulk.ColumnMappings.Add("fecha", "fecha");
                bulk.ColumnMappings.Add("anio", "anio");
                bulk.ColumnMappings.Add("mes", "mes");
                bulk.ColumnMappings.Add("dia", "dia");
                bulk.ColumnMappings.Add("trimestre", "trimestre");
                bulk.ColumnMappings.Add("nombre_mes", "nombre_mes");
                bulk.ColumnMappings.Add("nombre_dia", "nombre_dia");
                bulk.WriteToServer(dt);
            }
        }

        Console.WriteLine($"    → {dt.Rows.Count} fechas únicas cargadas ✓");
    }

    // ── DIM SUCURSAL ──────────────────────────────────────────
    public Dictionary<string, int> CargarDimSucursal(List<Venta> ventas)
    {
        Console.WriteLine("\n  Cargando dim_sucursal_ssis...");
        var mapa = new Dictionary<string, int>();
        var dt = new DataTable();
        dt.Columns.Add("nombre_sucursal", typeof(string));
        var vistos = new HashSet<string>();

        foreach (var v in ventas)
        {
            string sucursalKey = (v.Sucursal ?? "").Trim();
            if (string.IsNullOrEmpty(sucursalKey)) continue;
            if (vistos.Contains(sucursalKey)) continue;
            vistos.Add(sucursalKey);
            dt.Rows.Add(v.Sucursal);
        }

        using (var conn = new SqlConnection(_connStr))
        {
            conn.Open();
            using (var bulk = new SqlBulkCopy(conn))
            {
                bulk.DestinationTableName = "dbo.dim_sucursal_ssis";
                bulk.ColumnMappings.Add("nombre_sucursal", "nombre_sucursal");
                bulk.WriteToServer(dt);
            }

            var cmd = new SqlCommand(
                "SELECT id_sucursal_sk, nombre_sucursal FROM dbo.dim_sucursal_ssis", conn);
            using (var reader = cmd.ExecuteReader())
                while (reader.Read())
                    mapa[(string)reader["nombre_sucursal"]] = (int)reader["id_sucursal_sk"];
        }

        Console.WriteLine($"    → {dt.Rows.Count} sucursales cargadas ✓");
        return mapa;
    }

    // ── DIM METODO PAGO ───────────────────────────────────────
    public Dictionary<string, int> CargarDimMetodoPago(List<Venta> ventas)
    {
        Console.WriteLine("\n  Cargando dim_metodo_pago_ssis...");
        var mapa = new Dictionary<string, int>();
        var dt = new DataTable();
        dt.Columns.Add("metodo_pago", typeof(string));
        var vistos = new HashSet<string>();

        foreach (var v in ventas)
        {
            string metodoKey = (v.MetodoPago ?? "").Trim();
            if (string.IsNullOrEmpty(metodoKey)) continue;
            if (vistos.Contains(metodoKey)) continue;
            vistos.Add(metodoKey);
            dt.Rows.Add(v.MetodoPago);
        }

        using (var conn = new SqlConnection(_connStr))
        {
            conn.Open();
            using (var bulk = new SqlBulkCopy(conn))
            {
                bulk.DestinationTableName = "dbo.dim_metodo_pago_ssis";
                bulk.ColumnMappings.Add("metodo_pago", "metodo_pago");
                bulk.WriteToServer(dt);
            }

            var cmd = new SqlCommand(
                "SELECT id_metodo_pago_sk, metodo_pago FROM dbo.dim_metodo_pago_ssis", conn);
            using (var reader = cmd.ExecuteReader())
                while (reader.Read())
                    mapa[(string)reader["metodo_pago"]] = (int)reader["id_metodo_pago_sk"];
        }

        Console.WriteLine($"    → {dt.Rows.Count} métodos cargados ✓");
        return mapa;
    }

    // ── FACT VENTAS ───────────────────────────────────────────
    public void CargarFactVentas(
        List<Venta> ventas,
        Dictionary<int, int> mapaClientes,
        Dictionary<int, int> mapaProductos,
        Dictionary<string, int> mapaSucursales,
        Dictionary<string, int> mapaMetodos)
    {
        Console.WriteLine("\n  Cargando fact_ventas_ssis...");

        var dt = new DataTable();
        dt.Columns.Add("id_venta_odoo", typeof(int));
        dt.Columns.Add("id_fecha", typeof(int));
        dt.Columns.Add("id_cliente_sk", typeof(int));
        dt.Columns.Add("id_producto_sk", typeof(int));
        dt.Columns.Add("id_sucursal_sk", typeof(int));
        dt.Columns.Add("id_metodo_pago_sk", typeof(int));
        dt.Columns.Add("cantidad", typeof(int));
        dt.Columns.Add("precio_unitario", typeof(double));
        dt.Columns.Add("subtotal", typeof(double));
        dt.Columns.Add("iva", typeof(double));
        dt.Columns.Add("total", typeof(double));
        dt.Columns.Add("vendedor", typeof(string));
        dt.Columns.Add("sucursal", typeof(string));

        var factsVistos = new HashSet<string>();
        int duplicados = 0;

        foreach (var v in ventas)
        {
            if (string.IsNullOrEmpty(v.FechaVenta)) continue;
            int idFecha = int.Parse(DateTime.Parse(v.FechaVenta).ToString("yyyyMMdd"));

            if (!mapaClientes.TryGetValue(v.ClienteId, out int idClienteSk)) continue;
            mapaSucursales.TryGetValue(v.Sucursal ?? "", out int idSucursalSk);
            mapaMetodos.TryGetValue(v.MetodoPago ?? "", out int idMetodoSk);

            foreach (var d in v.Detalles ?? new List<DetalleVenta>())
            {
                string factKey = $"{v.Id}-{d.ProductoId}";

                if (factsVistos.Contains(factKey))
                {
                    Console.WriteLine($"    [DUPLICADO] Fact ignorado: venta {v.Id} - producto {d.ProductoId}");
                    duplicados++;
                    continue;
                }

                factsVistos.Add(factKey);

                if (!mapaProductos.TryGetValue(d.ProductoId, out int idProductoSk)) continue;

                dt.Rows.Add(
                    v.Id, idFecha, idClienteSk, idProductoSk,
                    idSucursalSk, idMetodoSk,
                    d.Cantidad, d.PrecioUnitario, d.Subtotal,
                    v.Iva, v.Total,
                    v.Vendedor, v.Sucursal
                );
            }
        }

        if (duplicados > 0)
            Console.WriteLine($"    [LIMPIEZA] {duplicados} filas duplicadas eliminadas en fact_ventas_ssis");

        using (var conn = new SqlConnection(_connStr))
        {
            conn.Open();
            using (var bulk = new SqlBulkCopy(conn))
            {
                bulk.BulkCopyTimeout = 120;
                bulk.DestinationTableName = "dbo.fact_ventas_ssis";
                bulk.ColumnMappings.Add("id_venta_odoo", "id_venta_odoo");
                bulk.ColumnMappings.Add("id_fecha", "id_fecha");
                bulk.ColumnMappings.Add("id_cliente_sk", "id_cliente_sk");
                bulk.ColumnMappings.Add("id_producto_sk", "id_producto_sk");
                bulk.ColumnMappings.Add("id_sucursal_sk", "id_sucursal_sk");
                bulk.ColumnMappings.Add("id_metodo_pago_sk", "id_metodo_pago_sk");
                bulk.ColumnMappings.Add("cantidad", "cantidad");
                bulk.ColumnMappings.Add("precio_unitario", "precio_unitario");
                bulk.ColumnMappings.Add("subtotal", "subtotal");
                bulk.ColumnMappings.Add("iva", "iva");
                bulk.ColumnMappings.Add("total", "total");
                bulk.ColumnMappings.Add("vendedor", "vendedor");
                bulk.ColumnMappings.Add("sucursal", "sucursal");
                bulk.WriteToServer(dt);
            }
        }

        Console.WriteLine($"    → {dt.Rows.Count} filas cargadas en fact_ventas_ssis ✓");
    }
}