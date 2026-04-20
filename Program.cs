using System;
using TechStore_ETL_API.ETL;

class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("");
        Console.WriteLine("TechStore ETL — API → SQL Server");
        Console.WriteLine("\n");

        try
        {
            // ── SERVICIOS ETL
            var extract = new ExtractService();
            var transform = new TransformService();
            var load = new LoadService();

            // ── EXTRACT: obtener datos crudos desde APIs
            var (clientesCrudos, productosCrudos, ventasCrudas) = extract.ExtraerTodo();

            // ── TRANSFORM: limpiar y deduplicar 
            var (clientes, productos, ventas) = transform.TransformarTodo(
                clientesCrudos, productosCrudos, ventasCrudas);

            // ── LOAD: cargar al Data Warehouse
            Console.WriteLine("\n[ LOAD ] Cargando datos en SQL Server...");
            load.TruncateAll();

            var mapaClientes = load.CargarDimCliente(clientes);
            var mapaProductos = load.CargarDimProducto(productos);
            load.CargarDimFecha(ventas);
            var mapaSucursales = load.CargarDimSucursal(ventas);
            var mapaMetodos = load.CargarDimMetodoPago(ventas);

            load.CargarFactVentas(
                ventas, mapaClientes, mapaProductos,
                mapaSucursales, mapaMetodos
            );

            Console.WriteLine("\n");
            Console.WriteLine("ETL COMPLETADO");
            Console.WriteLine("");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n[ERROR] {ex.Message}");
            Console.WriteLine(ex.StackTrace);
        }

        Console.WriteLine("\nPresiona Enter para salir...");
        Console.ReadLine();
    }
}