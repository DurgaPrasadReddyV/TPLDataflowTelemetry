using System.Diagnostics;
using System.Diagnostics.Metrics;
using TPLDataflowTelemetry.ApiService;

var builder = WebApplication.CreateBuilder(args);

// Add service defaults & Aspire client integrations.
builder.AddServiceDefaults();

// Add services to the container.
builder.Services.AddProblemDetails();

builder.Services.AddSingleton(new ActivitySource("SmartReturns.TplDataflow"));
builder.Services.AddSingleton(new Meter("SmartReturns.TplDataflow"));
builder.Services.AddSingleton<TplDataflowTracer>();

var app = builder.Build();

app.Lifetime.ApplicationStarted.Register(() =>
{
    // Start the orders pipeline when the application starts.
    var tracer = app.Services.GetRequiredService<TplDataflowTracer>();
    var cts = new CancellationTokenSource();
    Demo.RunOrdersPipeline(tracer, cts.Token).GetAwaiter().GetResult();
});

// Configure the HTTP request pipeline.
app.UseExceptionHandler();

string[] summaries = ["Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"];

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
    return forecast;
})
.WithName("GetWeatherForecast");

app.MapDefaultEndpoints();

app.Run();

record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}
