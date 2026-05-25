using TourDataOrchestrator.Api.Infrastructure;
using TourDataOrchestrator.Application.Abstractions;
using TourDataOrchestrator.Messaging.Extensions;
using TourDataOrchestrator.Storage.Extensions;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddMessaging(builder.Configuration);
builder.Services.AddStorage(builder.Configuration);

// TODO: zastąpić docelową implementacją z upstream systemu
builder.Services.AddSingleton<IScrapingResultAggregator, NullScrapingResultAggregator>();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapControllers();

await app.RunAsync();
