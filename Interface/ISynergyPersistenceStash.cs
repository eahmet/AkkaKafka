using Akka.Actor;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace AkkaKafka.Interface
{
    public interface ISynergyPersistenceStash 
    {
        Task ClearStash();

        Task Stash(object message, IActorRef sender);
        void Unstash();
        long GetStashedCount();

        long GetStashedCount(string instance);

        IEnumerable<object> GetStashedMessages();

        object GetStashedMessage();

        bool IsPersistenceQueueAlive();
    }
}
