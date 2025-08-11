var builder = DistributedApplication.CreateBuilder(args);

var apiService = builder.AddProject<Projects.TPLDataflowTelemetry_ApiService>("apiservice")
    .WithHttpHealthCheck("/health");

builder.Build().Run();
