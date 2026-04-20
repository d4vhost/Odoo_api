using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using TechStore_ETL_API;
using TechStore_ETL_API.Models;

public class ApiClient
{
    private readonly HttpClient _http;
    private readonly string _baseUrl;

    public ApiClient()
    {
        _http = new HttpClient();
        _baseUrl = Config.OdooBaseUrl;
        _http.DefaultRequestHeaders.Add("Accept", "application/json");
    }

    // Odoo usa JSON-RPC, hay que envolver el request
    private string EnviarJsonRpc(string endpoint)
    {
        var body = new
        {
            jsonrpc = "2.0",
            method = "call",
            id = 1,
            @params = new { }     // GET no necesita params
        };

        var content = new StringContent(
            JsonConvert.SerializeObject(body),
            Encoding.UTF8,
            "application/json"
        );

        var response = _http.PostAsync($"{_baseUrl}{endpoint}", content).Result;
        response.EnsureSuccessStatusCode();
        return response.Content.ReadAsStringAsync().Result;
    }

    public List<Cliente> GetClientes()
    {
        Console.WriteLine("  Extrayendo clientes desde API...");
        var json = EnviarJsonRpc("/api/clientes");
        var result = JObject.Parse(json)["result"];
        var data = result["data"].ToObject<List<Cliente>>();
        Console.WriteLine($"  → {data.Count} clientes extraídos");
        return data;
    }

    public List<Producto> GetProductos()
    {
        Console.WriteLine("  Extrayendo productos desde API...");
        var json = EnviarJsonRpc("/api/productos");
        var result = JObject.Parse(json)["result"];
        var data = result["data"].ToObject<List<Producto>>();
        Console.WriteLine($"  → {data.Count} productos extraídos");
        return data;
    }

    public List<Venta> GetVentas()
    {
        Console.WriteLine("  Extrayendo ventas desde API...");
        var json = EnviarJsonRpc("/api/ventas");
        var result = JObject.Parse(json)["result"];
        var data = result["data"].ToObject<List<Venta>>();
        Console.WriteLine($"  → {data.Count} ventas extraídas");
        return data;
    }
}