using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.WebUtilities;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddScoped<ICreateJobService, CreateJobService>();
builder.Services.AddScoped<IConversorService, ConversorService>();
// Add services to the container.

var app = builder.Build();

// Configure the HTTP request pipeline.

app.UseHttpsRedirection();

app.MapGet("/jobs", (string jobName)
    => new CreateJobResponse(jobName == "Expurgo" ? "ok" : jobName));
app.MapPost("/jobs", async (
        [FromServices] ICreateJobService service,
        [FromBody] CreateJobRequest request)
    => await service.Handle(request));

app.Run();

public interface ICreateJobService
{
    Task<CreateJobResponse> Handle(CreateJobRequest request);
}

public class CreateJobService(IConversorService conversorService) : ICreateJobService
{
    public async Task<CreateJobResponse> Handle(CreateJobRequest request)
    {
        var query = conversorService.Handle(request.JobName, request.CronExpression, request.DatabaseName,
            request.Schema,
            request.Table,
            request.Filters,
            request.Limit);
        var repository = new Repository();
        await repository.AddAsync(query);
        return new CreateJobResponse(query);
    }
}

public interface IConversorService
{
    string Handle(string jobName, string cronExpression, string databaseName, string schema, string table,
        CreateJobFilter[] filters, int limit);
}

public class ConversorService : IConversorService
{
    public string Handle(string jobName, string cronExpression, string databaseName, string schema, string table,
        CreateJobFilter[] filters, int limit)
    {
        limit = limit > Config.DefaultLimit ? limit : Config.DefaultLimit;
        var internalQuery = GetInternalQuery(schema, table, filters, limit);
        return
            $"SELECT cron.schedule('{databaseName}-{jobName}-job', '{cronExpression}', {internalQuery}, '{databaseName}');";
    }

    public string GetInternalQuery(string schema, string table,
        CreateJobFilter[] filters, int limit)
    {
        if (filters == null || filters.Length == 0) throw new ArgumentNullException(nameof(filters));
        var filterString = "";
        for (int i = 0; i < filters.Length; i++)
        {
            filterString += i > 1 ? "AND " : "";
            filterString += GetFilters(filters[i].Column, filters[i].Days);
        }

        return
            $"$$ with om_cte as (select om.\"Id\" from {schema}.\"{table}\" om WHERE {filterString} LIMIT {limit}) delete from {schema}.\"{table}\" om where om.\"Id\" in (select \"Id\" from om_cte);$$";
    }

    public string GetFilters(string coluna, int days)
    {
        days = days > Config.DefaultDays ? days : Config.DefaultDays;
        return $"om.\"{coluna}\" AT TIME ZONE 'UTC' < NOW() AT TIME ZONE 'UTC' - INTERVAL '{days} days'";
    }
}

public class Repository
{
    public Task<int> AddAsync(string query)
    {
        return Task.FromResult(1);
    }
}

public class CreateJobRequest
{
    public string JobName { get; set; }
    public string CronExpression { get; set; }
    public string DatabaseName { get; set; }
    public string Schema { get; set; }
    public string Table { get; set; }
    public int Limit { get; set; }
    public CreateJobCronExpression Timer { get; set; }
    public CreateJobFilter[] Filters { get; set; }
}

public class CreateJobFilter
{
    public string Column { get; set; }
    public int Days { get; set; }
}

public class CreateJobCronExpression
{
    public int Seconds { get; set; }
    public int Minutes { get; set; }
    public int Hours { get; set; }
    public int Days { get; set; }
}

public record CreateJobResponse(string Mensagem);

public static class Config
{
    public static readonly int DefaultDays = 60;
    public static readonly int DefaultLimit = 100;
}

public partial class Program
{
}
