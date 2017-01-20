using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System.Configuration;

namespace producer
{
    class Program
    {
        static void Main(string[] args)
        {
            do
            {
                Produce(GetKafkaBroker(), getTopicName());
                System.Threading.Thread.Sleep(3000);
            } while (true);
        }

        private static void Produce(string broker, string topic)
        {
            
            string[] temp = broker.Split(',');
            Uri[] url = new Uri[3];
            int index = 0;
            foreach (string item in temp)
            {
                url[index] = new Uri(item);
                index++;
            }
            var options = new KafkaOptions(url);
            var router = new BrokerRouter(options);
            var client = new Producer(router);

            var currentDatetime = DateTime.Now;
            var key = currentDatetime.Second.ToString();
            var events = new[] { new Message("Hello World " + currentDatetime, key) };
            client.SendMessageAsync(topic, events).Wait(2000);
            Console.WriteLine("Produced: Key: {0}. Message: {1}", key, events[0].Value.ToUtf8String());

            using (client) { }
        }

        private static string GetKafkaBroker()
        {
            string KafkaBroker = string.Empty;
            const string kafkaBrokerKeyName = "KafkaBroker";

            if (!ConfigurationManager.AppSettings.AllKeys.Contains(kafkaBrokerKeyName))
            {
                KafkaBroker = "http://localhost:9092";
            }
            else
            {
                KafkaBroker = ConfigurationManager.AppSettings[kafkaBrokerKeyName];
            }
            return KafkaBroker;
        }
        private static string getTopicName()
        {
            string TopicName = string.Empty;
            const string topicNameKeyName = "Topic";

            if (!ConfigurationManager.AppSettings.AllKeys.Contains(topicNameKeyName))
            {
                throw new Exception("Key \"" + topicNameKeyName + "\" not found in Config file -> configuration/AppSettings");
            }
            else
            {
                TopicName = ConfigurationManager.AppSettings[topicNameKeyName];
            }
            return TopicName;
        }
    }
}
