using TourDataOrchestrator.MockWorker.Configuration;
using TourDataOrchestrator.MockWorker.Services;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<MockWorkerOptions>(
    builder.Configuration.GetSection(MockWorkerOptions.SectionName));

builder.Services.AddHostedService<MockDataConsumerService>();

var host = builder.Build();
await host.RunAsync();
