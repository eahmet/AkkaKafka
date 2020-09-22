using Akka.Actor;
using Akka.Routing;
using AkkaKafka.Actors;
using AkkaKafka.Classes;
using Hyperion.Internal;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace AkkaKafka
{
    public class WorkerActor:PersistenceWorkerActor
    {
        public bool isIdle = true;
        public WorkerActor(List<KeyValuePair<string,string>> kafkaConfig):base(kafkaConfig)
        {
            Receive<StartCalculateMessage>(msg => Handle(msg));
            Receive<SetActorProcessingMessage>(mgs =>
            { 
               Become(BecomeProcessing);
            });

            //Context.System.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(1),TimeSpan.FromSeconds(2),Self,new SetActorProcessingMessage(),ActorRefs.NoSender);
        }
        private void Handle(StartCalculateMessage message)
        {
            Console.WriteLine($"{Self.Path.Uid.ToString()} message Recieved {message.Id}");
            //Become(BecomeProcessing);
        }
        private void HandleWithBecome(StartCalculateMessage message)
        {
            Thread.Sleep(100);
            Console.WriteLine($"{Self.Path.Uid.ToString()} message Recieved {message.Id}");
            Become(BecomeProcessing);
        }
       
        private void BecomeProcessing()
        {
            Stash.Unstash();

            Receive<StartCalculateMessage>(HandleWithBecome);
        }
        
        protected override void PreStart()
        {
            Console.WriteLine($"- [PreStart Worker Actor] {Self.Path.Uid.ToString()} is starting.");
            base.PreStart();
        }
    }
}
