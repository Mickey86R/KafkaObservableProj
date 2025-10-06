using KafkaObservableProj.Services;
using Microsoft.AspNetCore.Mvc;

namespace KafkaObservableProj.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class StatsController : ControllerBase
    {
        private IEventObserver Observer { get; init; }
        private ILogger<StatsController> Logger { get; init; }

        public StatsController(IEventObserver observer, ILogger<StatsController> logger)
        {
            Observer = observer;
            Logger = logger;
        }

        // GET /stats  -> возвращает текущий snapshot (не сбрасывает счётчики)
        [HttpGet]
        public ActionResult<IEnumerable<UserEventStat>> Get()
        {
            var snapshot = Observer.GetSnapshot();
            return Ok(snapshot);
        }

        // GET /stats  -> возвращает текущий snapshot (не сбрасывает счётчики)
        [HttpGet("{typeFilter}")]
        public ActionResult<IEnumerable<UserEventStat>> Get(string typeFilter)
        {
            var snapshot = Observer.GetSnapshot(typeFilter);
            return Ok(snapshot);
        }

        // POST /stats/flush -> принудительно сбросить накопленные значения в хранилище
        [HttpPost("flush")]
        public async Task<IActionResult> Flush()
        {
            Logger.LogInformation("Manual flush requested");
            await Observer.FlushAsync();
            return Accepted();
        }
    }
}
