using KafkaObservableProj.Services;
using Npgsql;
using System.Text.Json;

namespace KafkaObservableProj.Data
{
    public class DataStorage
    {
    }
    public interface IDataStorage
    {
        Task SaveAsync(IEnumerable<UserEventStat> stats);
    }

    public static class DataStorageFactory
    {
        public static IDataStorage Create(ILoggerFactory loggerFactory)
        {
            var conn = Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING");
            if (!string.IsNullOrWhiteSpace(conn))
                return new PostgresStorage(conn, loggerFactory.CreateLogger<PostgresStorage>());

            var path = Environment.GetEnvironmentVariable("STATS_FILE_PATH") ?? "user_event_stats.json";
            return new FileStorage(path, loggerFactory.CreateLogger<FileStorage>());
        }
    }

    public class PostgresStorage : IDataStorage
    {
        private readonly string _conn;
        private readonly ILogger<PostgresStorage> _logger;

        public PostgresStorage(string connectionString, ILogger<PostgresStorage> logger)
        {
            _conn = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _logger = logger;
        }

        public async Task SaveAsync(IEnumerable<UserEventStat> stats)
        {
            await using var conn = new NpgsqlConnection(_conn);
            await conn.OpenAsync();

            // ensure table
            var create = @"
                            CREATE TABLE IF NOT EXISTS user_event_stats (
                            user_id INT NOT NULL,
                            event_type VARCHAR(50) NOT NULL,
                            count BIGINT NOT NULL,
                            PRIMARY KEY (user_id, event_type)
                            );";

            await using (var c = new NpgsqlCommand(create, conn)) { await c.ExecuteNonQueryAsync(); }

            await using var tx = await conn.BeginTransactionAsync();
            try
            {
                foreach (var s in stats)
                {
                    var sql = @"
                                INSERT INTO user_event_stats (user_id, event_type, count)
                                VALUES (@u, @e, @c)
                                ON CONFLICT (user_id, event_type)
                                DO UPDATE SET count = user_event_stats.count + EXCLUDED.count;
                                ";
                    await using var cmd = new NpgsqlCommand(sql, conn, tx);
                    cmd.Parameters.AddWithValue("u", s.UserId);
                    cmd.Parameters.AddWithValue("e", s.EventType);
                    cmd.Parameters.AddWithValue("c", s.Count);
                    await cmd.ExecuteNonQueryAsync();
                }

                await tx.CommitAsync();
                _logger.LogInformation("Saved stats to Postgres");
            }
            catch
            {
                await tx.RollbackAsync();
                throw;
            }
        }
    }

    public class FileStorage : IDataStorage
    {
        private readonly string _path;
        private readonly ILogger<FileStorage> _logger;
        private readonly object _fileLock = new();

        public FileStorage(string path, ILogger<FileStorage> logger)
        {
            _path = path;
            _logger = logger;
        }

        public Task SaveAsync(IEnumerable<UserEventStat> stats)
        {
            lock (_fileLock)
            {
                var list = new List<UserEventStat>();
                if (File.Exists(_path))
                {
                    try
                    {
                        var existing = JsonSerializer.Deserialize<List<UserEventStat>>(File.ReadAllText(_path),
                            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                        if (existing != null) list.AddRange(existing);
                    }
                    catch
                    {
                        // ignore, перезапишем файл
                    }
                }

                // merge sums by key
                var map = new Dictionary<string, UserEventStat>();
                foreach (var item in list) map[$"{item.UserId}:{item.EventType}"] = item;
                foreach (var s in stats)
                {
                    var k = $"{s.UserId}:{s.EventType}";
                    if (map.TryGetValue(k, out var cur)) cur.Count += s.Count;
                    else map[k] = new UserEventStat { UserId = s.UserId, EventType = s.EventType, Count = s.Count };
                }

                var outList = new List<UserEventStat>(map.Values);
                File.WriteAllText(_path, JsonSerializer.Serialize(outList, new JsonSerializerOptions { WriteIndented = true }));
                _logger.LogInformation("Saved stats to file {File}", _path);
                return Task.CompletedTask;
            }
        }
    }
}
