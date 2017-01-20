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

namespace comsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            Consume(getKafkaBroker(), getTopicName());

        }

        private static void Consume(string broker, string topic)
        {
            string[] temp = broker.Split(',');
            Uri[] url =new Uri[3];
            int index = 0;
            foreach(string item in temp)
            {
                url[index] = new Uri(item);
                index++;
            }
            var options = new KafkaOptions(url);
            var router = new BrokerRouter(options);
            OffsetPosition[] off =new OffsetPosition[3];
            //off
            off[0] = new OffsetPosition(0, 9999);
            off[1] = new OffsetPosition(1, 9999);
            off[2] = new OffsetPosition(2, 9999);
            Consumer consumer = new Consumer(new ConsumerOptions(topic, router),off);
            //var tt2 = consumer.GetTopicOffsetAsync(topic, 2, -1);
            ////var tt = consumer.GetOffsetPosition();
            //var test = CreateOffsetFetchRequest(topic,0);
            //var test2 = CreateFetchRequest(topic, 0);
            ////consumer.SetOffsetPosition(new OffsetPosition(consumer.GetOffsetPosition()));
            ////Consume returns a blocking IEnumerable (ie: never ending stream)
            bool flag = false;
            //var t =consumer.Consume();
            //var tt = consumer.GetOffsetPosition();
            List<OffsetPosition> tt3;
            foreach (var message in consumer.Consume())
            {
                //if (!flag)
                //{
                    
                    tt3 = consumer.GetOffsetPosition();
                    OffsetPosition[] str = tt3.ToArray();
                    consumer.SetOffsetPosition(str);
                    //consumer.Dispose();
                    //break;
                    
                    flag = true;
                //}
                Console.WriteLine("1 Response: Partition {0},Offset {1} : {2}",
                    message.Meta.PartitionId, message.Meta.Offset, message.Value.ToUtf8String());
            }
            //consumer.Dispose();
            //OffsetPosition[] str = tt3.ToArray();
            //var consumer2 = new Consumer(new ConsumerOptions(topic, router), str);
            //foreach (var message in consumer2.Consume())
            //{

            //    Console.WriteLine("2 Response: Partition {0},Offset {1} : {2}",
            //        message.Meta.PartitionId, message.Meta.Offset, message.Value.ToUtf8String());
            //}

        }

        private static string getKafkaBroker()
        {
            string KafkaBroker = string.Empty;
            var KafkaBrokerKeyName = "KafkaBroker";

            if (!ConfigurationManager.AppSettings.AllKeys.Contains(KafkaBrokerKeyName))
            {
                KafkaBroker = "http://localhost:9092";
            }
            else
            {
                KafkaBroker = ConfigurationManager.AppSettings[KafkaBrokerKeyName];
            }
            return KafkaBroker;
        }

        private static string getTopicName()
        {
            string TopicName = string.Empty;
            var TopicNameKeyName = "Topic";

            if (!ConfigurationManager.AppSettings.AllKeys.Contains(TopicNameKeyName))
            {
                throw new Exception("Key \"" + TopicNameKeyName + "\" not found in Config file -> configuration/AppSettings");
            }
            else
            {
                TopicName = ConfigurationManager.AppSettings[TopicNameKeyName];
            }
            return TopicName;
        }
        public static OffsetFetchRequest CreateOffsetFetchRequest(string topic, int partitionId = 0)
        {
            return new OffsetFetchRequest
            {
                ConsumerGroup = "DefaultGroup",
                Topics = new List<OffsetFetch>(new[] 
        		                          {
        		                          	new OffsetFetch
        		                          	{
        		                          		Topic = topic,
        		                          		PartitionId = partitionId
        		                          	}
        		                          })
            };
        }
        public static FetchRequest CreateFetchRequest(string topic, int offset, int partitionId = 0)
        {
            return new FetchRequest
            {
                CorrelationId = 1,
                Fetches = new List<Fetch>(new[]
                        {
                            new Fetch
                                {
                                    Topic = topic,
                                    PartitionId = partitionId,
                                    Offset = offset
                                }
                        })
            };
        }

        public static OffsetRequest CreateOffsetRequest(string topic, int partitionId = 0, int maxOffsets = 1, int time = -1)
        {
            return new OffsetRequest
            {
                CorrelationId = 1,
                Offsets = new List<Offset>(new[]
                        {
                            new Offset
                                {
                                    Topic = topic,
                                    PartitionId = partitionId,
                                    MaxOffsets = maxOffsets,
                                    Time = time
                                }
                        })
            };
        }
    }
}
