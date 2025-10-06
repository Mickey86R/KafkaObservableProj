using KafkaObservableProj.DTO;
using KafkaObservableProj.Models;
using KafkaObservableProj.Services;
using Microsoft.AspNetCore.Mvc;

namespace KafkaObservableProj.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class StatsController : ControllerBase
    {
        private IEventObserver Observer { get; init; }

        public StatsController(IEventObserver observer)
        {
            Observer = observer;
        }

        // GET /stats
        [HttpGet]
        public ActionResult<IEnumerable<UserEventStat>> Get()
        {
            var snapshot = Observer.GetSnapshot();
            return Ok(snapshot);
        }

        // GET /stats/filter/typefilter  -> typefilter может быть ("click" и т.п.)
        [HttpGet("/filter")]
        public ActionResult<IEnumerable<UserEventStat>> Get(string typeFilter)
        {
            var snapshot = Observer.GetSnapshot(typeFilter);
            return Ok(snapshot);
        }

        // GET /stats/timefilter -> 
        [HttpGet("/timefilter")] ///{from:datetime},{to:datetime}
        public ActionResult<IEnumerable<UserEvent>> Get(DateTime? from, DateTime? to)
        {
            var snapshot = Observer.GetSnapshot(from, to);
            return Ok(snapshot);
        }
    }
}
