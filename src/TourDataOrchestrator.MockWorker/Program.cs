using TourDataOrchestrator.MockWorker.Configuration;
using TourDataOrchestrator.MockWorker.Services;
using TourDataOrchestrator.Storage.Extensions;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<MockWorkerOptions>(
    builder.Configuration.GetSection(MockWorkerOptions.SectionName));

builder.Services.AddStorage(builder.Configuration);
builder.Services.AddHostedService<MockDataConsumerService>();

var host = builder.Build();
await host.RunAsync();
