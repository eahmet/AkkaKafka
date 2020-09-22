using Akka.Actor;
using AkkaKafka.Interface;
using AkkaKafka.Stash;
using System;
using System.Collections.Generic;
using System.Text;

namespace AkkaKafka.Actors
{
    public class PersistenceRouterActor:ReceiveActor
    {
        public ISynergyPersistenceStash Stash { get; private set; } = null;
        public List<KeyValuePair<string,string>> KafkaConfig=null;
        public PersistenceRouterActor():base()
        {

        }
        public PersistenceRouterActor(List<KeyValuePair<string,string>> kafkaConfig):base()
        {
            KafkaConfig=kafkaConfig;
        }
        protected override void PreStart()
        {
            Stash = new SynergyPersistenceStash(Context,KafkaConfig);
            //Stash = new SynergyPersistenceStashRabbitMq(Context,KafkaConfig);
            base.PreStart();
        }
    }
}
