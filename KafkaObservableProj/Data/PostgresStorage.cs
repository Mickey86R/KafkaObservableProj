using KafkaObservableProj.Models;
using KafkaObservableProj.Services;
using Npgsql;

namespace KafkaObservableProj.Data
{
    public class PostgresStorage : IDataStorage
    {
        private string Conn { get; init; }
        private ILogger<PostgresStorage> Logger { get; init; }

        public PostgresStorage(string connectionString, ILogger<PostgresStorage> logger)
        {
            Conn = connectionString;
            Logger = logger;
        }

        public async Task SaveAsync(IEnumerable<UserEventStat> stats)
        {
            await using var conn = new NpgsqlConnection(Conn);
            await conn.OpenAsync();

            // ensure table
            var create = @"
                            CREATE TABLE IF NOT EXISTS ""KafkaTestSchema"".user_event_stats (
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
                                INSERT INTO ""KafkaTestSchema"".user_event_stats (user_id, event_type, count)
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
                Logger.LogInformation("Saved stats to Postgres");
            }
            catch
            {
                await tx.RollbackAsync();
                throw;
            }
        }

        public async Task<bool> CheckConnect()
        {
            try
            {
                await using var conn = new NpgsqlConnection(Conn);
                await conn.OpenAsync();
            }
            catch
            {
                Logger.LogInformation("Error in the database connection string. Statistics will be saved to a local file");
                return false;
            }

            return true;
        }
    }
}
