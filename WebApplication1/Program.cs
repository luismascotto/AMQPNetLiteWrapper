using Amqp;
using AMQPNetLiteWrapper;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

var app = builder.Build();

// Load appsettings.json
var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
    .Build();

// Configure the HTTP request pipeline.

var summaries = new[]
{
    "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
};

app.MapGet("/weatherforecast", () =>
{
    var forecast = Enumerable.Range(1, 5).Select(index =>
        new WeatherForecast
        (
            DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
            Random.Shared.Next(-20, 55),
            summaries[Random.Shared.Next(summaries.Length)]
        ))
        .ToArray();


    var connection = AmqpHelper.Instance.GetSenderConnection(configuration.GetValue<string>("AmqpUrl")!, "forecast");
    Session? session = null;
    SenderLink? sender = null;
    try
    {
        session = new(connection);
        sender = new SenderLink(session, "forecast", "forecast");
        sender.Send(new Message(JsonSerializer.Serialize(forecast)), TimeSpan.FromMilliseconds(3000));
    }
    catch (Exception ex)
    {
      ConsoleDbg.WriteLine($"Erro AMQP {ex}", ConsoleColor.Red);
    }
    finally
    {
        session?.Close();
        sender?.Close();    
    }

    return forecast;
});

app.Run();

internal record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}
