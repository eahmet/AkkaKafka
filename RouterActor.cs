using Akka.Actor;
using Akka.Routing;
using AkkaKafka.Actors;
using AkkaKafka.Classes;
using System;
using System.Collections.Generic;
using System.Text;

namespace AkkaKafka
{
    public class RouterActor:PersistenceRouterActor
    {
        private readonly IActorRef _workerActor = null;
        public RouterActor(List<KeyValuePair<string,string>> kafkaConfig):base(kafkaConfig)
        {
            _workerActor = Context.ActorOf(Props.Create<WorkerActor>(kafkaConfig).WithRouter(new RoundRobinPool(20)),"workerActor");
            Receive<StartCalculateMessage>(Handle);

            // Terminated message handler for child actors
            Receive<Terminated>(t => t.ActorRef.Equals(_workerActor), msg =>
            {
               Console.WriteLine("Actor is terminated");
            });

        }

        private void Handle(StartCalculateMessage message)
        {
            var context = Context;
            var sender = Sender;
            var self = Self;
            
            
            if(!Stash.IsPersistenceQueueAlive())    
            { 
                Stash.Stash(message,sender);
                var setActorProcessing = new SetActorProcessingMessage();
                _workerActor.Tell(new Broadcast(setActorProcessing), Sender);
            }
            else
            {  
                _workerActor.Tell(message, Sender); 
            }
            
        }
        protected override void PreStart()
        {
            Console.WriteLine($"- [PreStart Router Actor] {Self.Path.Uid.ToString()} is starting.");
            base.PreStart();
        }
    }
}
