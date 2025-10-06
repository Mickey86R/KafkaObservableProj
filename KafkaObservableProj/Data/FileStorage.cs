using KafkaObservableProj.Models;
using KafkaObservableProj.Services;
using System.Text.Json;

namespace KafkaObservableProj.Data
{
    public class FileStorage : IDataStorage
    {
        private string Path { get; init; }
        private ILogger<FileStorage> Logger { get; init; }

        public FileStorage(string path, ILogger<FileStorage> logger)
        {
            Path = path;
            Logger = logger;
        }

        public Task SaveAsync(IEnumerable<UserEventStat> stats)
        {
            var list = new List<UserEventStat>();

            if (File.Exists(Path))
            {
                try
                {
                    var serializeOptions = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
                    var existing = JsonSerializer.Deserialize<List<UserEventStat>>(File.ReadAllText(Path), serializeOptions);

                    if (existing != null) list.AddRange(existing);
                }
                catch
                {
                    // ignore, перезапишем файл
                }
            }

            var map = new Dictionary<string, UserEventStat>();

            foreach (var item in list)
            {
                var key = $"{item.UserId}:{item.EventType}";
                map[key] = item;
            }

            foreach (var s in stats)
            {
                var key = $"{s.UserId}:{s.EventType}";

                if (map.TryGetValue(key, out var cur))
                    cur.Count += s.Count;

                else
                    map[key] = new UserEventStat
                    {
                        UserId = s.UserId,
                        EventType = s.EventType,
                        Count = s.Count
                    };
            }

            var outList = new List<UserEventStat>(map.Values);

            File.WriteAllText(Path, JsonSerializer.Serialize(outList, new JsonSerializerOptions { WriteIndented = true }));

            Logger.LogInformation("Saved stats to file {File}", Path);

            return Task.CompletedTask;

        }
    }
}
