using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RdKafka;
namespace RDkafka_producer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            string brokerList = "26.2.4.171:9092,26.2.4.172:9092,26.2.4.173:9092";
            string topicName = "test_topic";


            
            var topicConfig = new TopicConfig
            {
                //CustomPartitioner = (top, key, cnt) =>
                //{
                //    var kt = (key != null) ? Encoding.UTF8.GetString(key, 0, key.Length) : "(null)";

                //    int partition = 0;
                //    if (key != null)
                //    {
                //        partition = key.Length % cnt;
                //    }

                //    bool available = top.PartitionAvailable(partition);
                //    Console.WriteLine("Partitioner topic: {"+top.Name+"} key: {"+kt+"} partition count: {"+cnt+"} -> {"+partition+"} {"+available+"}");
                //    return partition;
                //}
            };

            using (Producer producer = new Producer(brokerList))
            using (Topic topic = producer.Topic(topicName, topicConfig))
            {
                Console.WriteLine("{"+producer.Name+"} producing on {"+topic.Name+"}. q to exit.");

                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    byte[] data = Encoding.UTF8.GetBytes(text);
                    byte[] key = null;
                    // Use the first word as the key
                    int index = text.IndexOf(" ");
                    if (index != -1)
                    {
                        key = Encoding.UTF8.GetBytes(text.Substring(0, index));
                    }

                    Task<DeliveryReport> deliveryReport = topic.Produce(data);
                    var unused = deliveryReport.ContinueWith(task =>
                    {
                        Console.WriteLine("Partition: {"+task.Result.Partition+"}, Offset: {"+task.Result.Offset+"}");
                    });
                }
            }
        }
    }
}
