using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;

namespace Integration;

public class IntegrationTestWebFactory : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
        => builder.UseEnvironment("Testing");
}

public abstract class IntegrationTests
{
    protected HttpClient ApiClient { get; }

    protected IntegrationTests()
    {
        var factory = new IntegrationTestWebFactory();
        ApiClient = factory.CreateDefaultClient();
    }

    protected async Task<T?> GetAsync<T>(string urlPath)
    {
        var result = await ApiClient.GetAsync(urlPath);
        return await result.Content.ReadFromJsonAsync<T>();
    }

    protected async Task<HttpResponseMessage> PostAsync<T>(string urlPath, T content)
    {
        var commandSerialize = JsonSerializer.Serialize(content);
        var requestBody = new StringContent(commandSerialize, Encoding.UTF8, "application/json");
        return await ApiClient.PostAsync(urlPath, requestBody);
    }

    protected async Task<V?> PostAsyncWithReturn<V,T>(string urlPath, T content)
    {
        var result = await PostAsync(urlPath, content);
        return await result.Content.ReadFromJsonAsync<V>();
    }
}

public class Tests : IntegrationTests
{
    [Fact]
    public async Task GetWithJobId_Succefully_Response()
    {
        var response = await GetAsync<CreateJobResponse>("/jobs");
        response!.Mensagem.Should().Be("ok");
    }

    [Fact]
    public async Task Post_Succefully_Response()
    {
        var expectedMessage = @"SELECT cron.schedule('internal_core-expurgo-job', '*/5 * * * *', $$ with om_cte as (select om.""Id"" from dbo.""message_outbox"" om WHERE om.""coluna_1"" AT TIME ZONE 'UTC' < NOW() AT TIME ZONE 'UTC' - INTERVAL '95 days' LIMIT 110) delete from dbo.""message_outbox"" om where om.""Id"" in (select ""Id"" from om_cte);$$, 'internal_core');";
        var command = new CreateJobRequest()
        {
            JobName = "expurgo",
            CronExpression = "*/5 * * * *",
            DatabaseName = "internal_core",
            Schema = "dbo",
            Table = "message_outbox",
            Limit = 110,
            Timer = new CreateJobCronExpression() { Days = 1 },
            Filters = [new CreateJobFilter() { Column = "coluna_1", Days = 95 }]
        };
        var responsePost = await PostAsyncWithReturn<CreateJobResponse, CreateJobRequest>("/jobs", command);
        responsePost!.Mensagem.Should().Be(expectedMessage);
    }

    [Fact]
    public async Task Post_WithoutAnyData_Response()
    {
        var command = new CreateJobRequest();
        var response = await PostAsync("/jobs", command);
        response.EnsureSuccessStatusCode();
//        var response = await result.Content.ReadFromJsonAsync<CreateJobResponse>();
//        response!.Mensagem.Should().Be("ok");
    }

    [Fact]
    public async Task Post_With_JobName_Response()
    {
        var command = new CreateJobRequest()
        {
            JobName = "test",
        };
        var response = await PostAsync("/jobs", command);
        response.EnsureSuccessStatusCode();
//        var response = await result.Content.ReadFromJsonAsync<CreateJobResponse>();
//        response!.Mensagem.Should().Be("ok");
    }

    [Theory]
    [InlineData("JobName", "databaseName")]
    [InlineData("JobName", null)]
    [InlineData("JobName", "")]
    [InlineData(null, "databaseName")]
    [InlineData(null, null)]
    [InlineData(null, "")]
    [InlineData("", "databaseName")]
    [InlineData("", null)]
    [InlineData("", "")]
    public async Task Post_With_Builder_Response(string jobName, string databaseName)
    {
        var command = Builder(jobName, databaseName);
        var response = await PostAsync("/jobs", command);
        response.EnsureSuccessStatusCode();
    }

    private CreateJobRequest Builder(string jobName, string databaseName)
    {
        return new CreateJobRequest()
        {
            JobName = jobName,
            DatabaseName = databaseName,
        };
    }
}
/*
using System.Runtime.CompilerServices;
using Bogus;
using Cronos;
using FluentAssertions;

namespace CronJob.UnitTests;

public class CronControllerTests
{
    [Fact]
    public void CreateJobRequest_Test()
    {
        var timer = new Faker<CreateJobCronExpression>()
            .RuleFor(x => x.Days, f => f.Random.Int())
            .Generate();
        var filter = new Faker<CreateJobFilter>()
            .RuleFor(x => x.Days, f => f.Random.Int())
            .Generate(2)
            .ToArray();
        var request = new Faker<CreateJobRequest>()
            .RuleFor(x => x.JobName, f => f.Lorem.Word())
            .RuleFor(x => x.DatabaseName, f => f.Lorem.Word())
            .RuleFor(x => x.Schema, f => f.Lorem.Word())
            .RuleFor(x => x.Limit, f => f.Random.Int())
            .RuleFor(x => x.Timer, timer)
            .RuleFor(x => x.Filters, filter);
    }
}

public class CronRepositoryTests
{
    [Fact]
    public async Task CronRepository_Create_Schedule_Succefully()
    {
        var expectedResponse = 1;
        var repository = new Repository();
        var response = await repository.AddAsync("Corretamente");
        response.Should().Be(expectedResponse);
    }

    [Fact]
    public async Task CronService_Create_Schedule_Succefully()
    {
        var expectedResponse =
            "SELECT cron.schedule('expandable-expurgo-job', '0 3 * * *', $$ with om_cte as (select om.\"Id\" from dbo.\"person\" om WHERE om.\"SentAt\" AT TIME ZONE 'UTC' < NOW() AT TIME ZONE 'UTC' - INTERVAL '60 days' LIMIT 100) delete from dbo.\"person\" om where om.\"Id\" in (select \"Id\" from om_cte);$$, 'expandable');";
        var service = new Service();
        var response = await service.AddAsync();
        response.Should().Be(expectedResponse);
    }

    [Fact]
    public async Task CronService_Create_Schedule_with_60Days_When_MinorDays()
    {
        var expectedResponse =
            "SELECT cron.schedule('expandable-expurgo-job', '0 3 * * *', $$ with om_cte as (select om.\"Id\" from dbo.\"person\" om WHERE om.\"SentAt\" AT TIME ZONE 'UTC' < NOW() AT TIME ZONE 'UTC' - INTERVAL '60 days' LIMIT 100) delete from dbo.\"person\" om where om.\"Id\" in (select \"Id\" from om_cte);$$, 'expandable');";
        var service = new Service();
        var response = await service.AddAsync();
        response.Should().Be(expectedResponse);
    }

    [Fact]
    public async Task CronService_Create_Schedule_with_MoreThan60Days()
    {
        var expectedResponse =
            "SELECT cron.schedule('expandable-expurgo-job', '0 3 * * *', $$ with om_cte as (select om.\"Id\" from dbo.\"person\" om WHERE om.\"SentAt\" AT TIME ZONE 'UTC' < NOW() AT TIME ZONE 'UTC' - INTERVAL '60 days' LIMIT 100) delete from dbo.\"person\" om where om.\"Id\" in (select \"Id\" from om_cte);$$, 'expandable');";
        var service = new Service();
        var response = await service.AddAsync();
        response.Should().Be(expectedResponse);
    }
}

public class ConversorTests
{
    private readonly Conversor _conversor;

    public ConversorTests()
    {
        _conversor = new Conversor();
    }

    [Fact]
    public void Conversor_handle_Schedule_Succefully()
    {
        var expectedResponse =
            "SELECT cron.schedule('Corretamente-expurgo-account-job', '0 3 * * *', $$ with om_cte as (select om.\"Id\" from dbo.\"accounts\" om WHERE om.\"SentAt\" AT TIME ZONE 'UTC' < NOW() AT TIME ZONE 'UTC' - INTERVAL '60 days' LIMIT 100) delete from dbo.\"accounts\" om where om.\"Id\" in (select \"Id\" from om_cte);$$, 'Corretamente');";
        var request = new RequestBuilder().Generate();
        var response = _conversor.Handle(request.JobName, request.CronExpression, request.DatabaseName, request.Schema,
            request.Table, request.Days, request.Limit);
        response.Should().Be(expectedResponse);
    }

    [Fact]
    public void Conversor_handle_Schedule_With_DaysMinorThanDefaultDay()
    {
        var expectedResponse =
            "SELECT cron.schedule('Corretamente-expurgo-account-job', '0 3 * * *', $$ with om_cte as (select om.\"Id\" from dbo.\"accounts\" om WHERE om.\"SentAt\" AT TIME ZONE 'UTC' < NOW() AT TIME ZONE 'UTC' - INTERVAL '60 days' LIMIT 100) delete from dbo.\"accounts\" om where om.\"Id\" in (select \"Id\" from om_cte);$$, 'Corretamente');";
        var request = new RequestBuilder()
            .WithDaysMinorThanDefaultDay()
            .Generate();
        var response = _conversor.Handle(request.JobName, request.CronExpression, request.DatabaseName, request.Schema,
            request.Table, request.Days, request.Limit);
        response.Should().Be(expectedResponse);
    }

    [Fact]
    public void Conversor_handle_Schedule_With_DayEqualOrGreaterThanDefaultDay()
    {
        var expectedResponse =
            "SELECT cron.schedule('Corretamente-expurgo-account-job', '0 3 * * *', $$ with om_cte as (select om.\"Id\" from dbo.\"accounts\" om WHERE om.\"SentAt\" AT TIME ZONE 'UTC' < NOW() AT TIME ZONE 'UTC' - INTERVAL '61 days' LIMIT 100) delete from dbo.\"accounts\" om where om.\"Id\" in (select \"Id\" from om_cte);$$, 'Corretamente');";
        var request = new RequestBuilder()
            .WithDaysGreaterThanDefaultDay()
            .Generate();
        var response = _conversor.Handle(request.JobName, request.CronExpression, request.DatabaseName, request.Schema,
            request.Table, request.Days, request.Limit);
        response.Should().Be(expectedResponse);
    }

    [Fact]
    public void Conversor_handle_Schedule_With_LimitsMinorThanDefaultLimit()
    {
        var expectedResponse =
            "SELECT cron.schedule('Corretamente-expurgo-account-job', '0 3 * * *', $$ with om_cte as (select om.\"Id\" from dbo.\"accounts\" om WHERE om.\"SentAt\" AT TIME ZONE 'UTC' < NOW() AT TIME ZONE 'UTC' - INTERVAL '60 days' LIMIT 100) delete from dbo.\"accounts\" om where om.\"Id\" in (select \"Id\" from om_cte);$$, 'Corretamente');";
        var request = new RequestBuilder()
            .WithLimitMinorThanDefaultLimit()
            .Generate();
        var response = _conversor.Handle(request.JobName, request.CronExpression, request.DatabaseName, request.Schema,
            request.Table, request.Days, request.Limit);
        response.Should().Be(expectedResponse);
    }

    [Fact]
    public void Conversor_handle_Schedule_With_LimitEqualOrGreaterThanDefaultLimit()
    {
        var expectedResponse =
            "SELECT cron.schedule('Corretamente-expurgo-account-job', '0 3 * * *', $$ with om_cte as (select om.\"Id\" from dbo.\"accounts\" om WHERE om.\"SentAt\" AT TIME ZONE 'UTC' < NOW() AT TIME ZONE 'UTC' - INTERVAL '60 days' LIMIT 101) delete from dbo.\"accounts\" om where om.\"Id\" in (select \"Id\" from om_cte);$$, 'Corretamente');";
        var request = new RequestBuilder()
            .WithLimitGreaterThanDefaultLimit()
            .Generate();
        var response = _conversor.Handle(request.JobName, request.CronExpression, request.DatabaseName, request.Schema,
            request.Table, request.Days, request.Limit);
        response.Should().Be(expectedResponse);
    }
}

public sealed class RequestBuilder : Faker<Request>
{
    public RequestBuilder()
    {
        RuleFor(x => x.JobName, f => "expurgo-account");
        RuleFor(x => x.CronExpression, f => "0 3 * * *");
        RuleFor(x => x.DatabaseName, f => "Corretamente");
        RuleFor(x => x.Schema, f => "dbo");
        RuleFor(x => x.Table, f => "accounts");
        RuleFor(x => x.Days, f => Config.DefaultDays);
        RuleFor(x => x.Limit, f => Config.DefaultLimit);
    }

    public RequestBuilder WithLimitMinorThanDefaultLimit()
    {
        RuleFor(x => x.Limit, f => f.Random.Number(Config.DefaultLimit - 1));
        return this;
    }

    public Faker<Request> WithLimitGreaterThanDefaultLimit()
    {
        RuleFor(x => x.Limit, f => Config.DefaultLimit + 1);
        return this;
    }

    public RequestBuilder WithDaysMinorThanDefaultDay()
    {
        RuleFor(x => x.Days, f => f.Random.Number(Config.DefaultDays));
        return this;
    }

    public RequestBuilder WithDaysGreaterThanDefaultDay()
    {
        RuleFor(x => x.Days, f => Config.DefaultDays + 1);
        return this;
    }
}

public class CronConvertTests
{
    [Fact]
    public void When_CronCorvert_Then_Correct_Output()
    {
        // Arrange
        string cron1 = "15 10 * * *";
        // Act
        CronSchedule schedule1 = CronConverter.Parse(cron1);
        // Assert
        schedule1.Second.Equals(15);
        schedule1.Minute.Equals(10);
        schedule1.Hour.Equals(0);
        schedule1.DayOfMonth.Equals(0);
        schedule1.Month.Equals(0);
        schedule1.DayOfWeek.Equals(0);
    }

    [Fact]
    public void ToCronString_WithFull5PartSchedule_ReturnsCorrectString()
    {
        // Arrange
        var schedule = new CronSchedule
        {
            Minute = "0",
            Hour = "22",
            DayOfMonth = "15",
            Month = "10",
            DayOfWeek = "3" // Quarta-feira
        };
        var expectedCron = "0 22 15 10 3";

        // Act
        var result = CronConverter.ToCronExpression(schedule);

        // Assert
        Assert.Equal(expectedCron, result);
    }
}

public class CronSchedule
{
    public string? Second { get; set; } = "0"; // Padrão
    public string? Minute { get; set; } = "*";
    public string? Hour { get; set; } = "*";
    public string? DayOfMonth { get; set; } = "*";
    public string? Month { get; set; } = "*";
    public string? DayOfWeek { get; set; } = "*";

    public override string ToString()
    {
        return
            $"Minute: '{Minute}', Hour: '{Hour}', DayOfMonth: '{DayOfMonth}', Month: '{Month}', DayOfWeek: '{DayOfWeek}', Second: '{Second}'";
    }
}

public static class CronConverter
{
    public static CronSchedule Parse(string cronExpression, CronFormat format = CronFormat.Standard)
    {
        // 1. Validação usando a biblioteca Cronos
        // Isso garante que a string está em um formato válido antes de continuarmos.
        CronExpression.Parse(cronExpression, format);

        // 2. Divisão da string em partes
        string?[] parts = cronExpression.Split(' ', StringSplitOptions.RemoveEmptyEntries);

        var schedule = new CronSchedule();

        if (parts.Length == 5) // Formato padrão sem segundos
        {
            schedule.Minute = parts[0];
            schedule.Hour = parts[1];
            schedule.DayOfMonth = parts[2];
            schedule.Month = parts[3];
            schedule.DayOfWeek = parts[4];
            // O segundo não está presente, então mantemos o padrão "0"
        }
        else if (parts.Length == 6) // Formato com segundos
        {
            schedule.Second = parts[0];
            schedule.Minute = parts[1];
            schedule.Hour = parts[2];
            schedule.DayOfMonth = parts[3];
            schedule.Month = parts[4];
            schedule.DayOfWeek = parts[5];
        }
        else
        {
            // A validação do Cronos já deve ter prevenido isso, mas é uma boa prática ter.
            throw new ArgumentException("A expressão Cron deve ter 5 ou 6 partes separadas por espaço.");
        }

        return schedule;
    }

    public static string ToCronExpression(CronSchedule schedule, bool includeSeconds = false, bool validate = true)
    {
        if (schedule == null)
        {
            throw new ArgumentNullException(nameof(schedule));
        }

        // Se uma propriedade for nula/vazia, usa "*" como padrão.
        // Para segundos, "0" é um padrão mais seguro se estiverem incluídos.
        var minute = !string.IsNullOrWhiteSpace(schedule.Minute) ? schedule.Minute : "*";
        var hour = !string.IsNullOrWhiteSpace(schedule.Hour) ? schedule.Hour : "*";
        var dayOfMonth = !string.IsNullOrWhiteSpace(schedule.DayOfMonth) ? schedule.DayOfMonth : "*";
        var month = !string.IsNullOrWhiteSpace(schedule.Month) ? schedule.Month : "*";
        var dayOfWeek = !string.IsNullOrWhiteSpace(schedule.DayOfWeek) ? schedule.DayOfWeek : "*";

        var parts = new List<string?>();

        if (includeSeconds)
        {
            var second = !string.IsNullOrWhiteSpace(schedule.Second) ? schedule.Second : "0";
            parts.Add(second);
        }

        parts.AddRange(new[] { minute, hour, dayOfMonth, month, dayOfWeek });

        string cronString = string.Join(" ", parts);

        // Passo de validação opcional, mas altamente recomendado
        if (validate)
        {
            try
            {
                var format = includeSeconds ? CronFormat.IncludeSeconds : CronFormat.Standard;
                CronExpression.Parse(cronString, format);
            }
            catch (CronFormatException ex)
            {
                // Lança uma exceção mais clara, indicando que a entrada (o objeto) estava errada.
                throw new ArgumentException(
                    $"As propriedades do objeto CronSchedule resultaram em uma expressão inválida: '{cronString}'. Detalhes: {ex.Message}",
                    nameof(schedule), ex);
            }
        }

        return cronString;
    }
}

public interface ICronJobService
{
    Task Adicionar();
}

public class ScheduleQueryBuilder
{
    public ScheduleQueryBuilder InternalQuery()
    {
        return this;
    }

    public ScheduleQueryBuilder ScheduleQuery()
    {
        return this;
    }

    public ScheduleQueryBuilder Builder()
    {
        return this;
    }
}

public record CreateCronJobRequest(
    string JobName,
    string DatabaseName,
    string TableName,
    string ColumnName,
    CreateCronJobSchedule Schedule);

public record CreateCronJobSchedule(
    int? Second,
    int? Minute,
    int? Hour,
    int? DayOfMonth,
    int? Month,
    int? DayOfWeek
);

public static class Mapper
{
    public static CronSchedule FromRequest(this CreateCronJobSchedule request)
    {
        return new CronSchedule()
        {
            Second = request.Second.HasValue ? request.Second.Value.ToString() : null,
            Minute = request.Minute.HasValue ? request.Minute.Value.ToString() : null,
            Hour = request.Hour.HasValue ? request.Hour.Value.ToString() : null,
            DayOfMonth = request.DayOfMonth.HasValue ? request.DayOfMonth.Value.ToString() : null,
            Month = request.Month.HasValue ? request.Month.Value.ToString() : null,
            DayOfWeek = request.DayOfWeek.HasValue ? request.DayOfWeek.Value.ToString() : null,
        };
    }
}

public class CronJobHandler(ICronJobService service)
{
    public Task Adicionar(CreateCronJobRequest request)
    {
        var cronExpression = CronConverter.ToCronExpression(request.Schedule.FromRequest());

        // string hour = "25";
        // if (int.TryParse(hour, out int hours))
        //     if (hours > 23)
        //         throw new ArgumentOutOfRangeException("Hora fora do intervalo permitido.");
        // var queryBuilder = new ScheduleQueryBuilder()
        //     .InternalQuery()
        //     .ScheduleQuery();
        // await service.Adicionar();
        return Task.CompletedTask;
    }
}
*/
