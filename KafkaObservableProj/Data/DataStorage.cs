using KafkaObservableProj.Models;
using KafkaObservableProj.Services;
using Npgsql;
using System.Text.Json;

namespace KafkaObservableProj.Data
{
    public interface IDataStorage
    {
        Task SaveAsync(IEnumerable<UserEventStat> stats);
    }

    public static class DataStorageFactory
    {
        /// <summary>
        /// Генерирует экземпляр IDataStorage. 
        /// Если строки подключения нет или она указана неверно, 
        /// статистика будет сохраняться локально
        /// </summary>
        /// <param name="loggerFactory"></param>
        /// <returns></returns>
        public static IDataStorage Create(ILoggerFactory loggerFactory)
        {
            var conn = Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING", EnvironmentVariableTarget.User);
            if (!string.IsNullOrWhiteSpace(conn))
            {
                var pgStorage = new PostgresStorage(conn, loggerFactory.CreateLogger<PostgresStorage>());

                if (pgStorage.CheckConnect().Result) return pgStorage;
            }

            var path = Environment.GetEnvironmentVariable("STATS_FILE_PATH", EnvironmentVariableTarget.User) ?? "user_event_stats.json";

            return new FileStorage(path, loggerFactory.CreateLogger<FileStorage>());
        }
    }

    
}
