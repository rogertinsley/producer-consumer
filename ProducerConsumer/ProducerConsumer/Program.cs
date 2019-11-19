using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace ProducerConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var cancelTokenSource = new CancellationTokenSource();
            CancellationToken cancelToken = cancelTokenSource.Token;

            // Thread-safe, concurrent queue blocks when empty
            var queue = new BlockingCollection<int>();
            var inflight = new ConcurrentDictionary<int, int>(); // WF Actions

            // Consumer Thread
            Task.Run(() =>
            {
                Thread.Sleep(5000);
                while (!queue.IsCompleted)
                {
                    Console.WriteLine($"[{DateTime.Now.ToLongTimeString()}] InFlight Count: {inflight.Count}");

                    if (inflight.Count >= 10)
                    {
                        Task.Delay(1000, cancelToken)
                            .Wait(cancelToken);
                    }
                    else
                    {
                        if (!queue.TryTake(out int item, 2000, cancelToken))
                        {
                            continue;
                        }

                        inflight.AddOrUpdate(item, item, (k, v) => v);
                        Task.Run(() =>
                        {
                            try
                            {
                                Process(item);
                            }
                            finally
                            {
                                inflight.TryRemove(item, out int value);
                            }
                        }, cancelToken);
                    }
                }
            }, cancelToken);

            // Producer loop
            while (true)
            {
                for (int i = 1; i <= 1000; i++)
                {
                    Console.WriteLine($"[{DateTime.Now.ToLongTimeString()}] Added {i} to queue");
                    queue.Add(i, cancelToken);
                }

                Thread.Sleep(60000);
            }
        }

        private static void Process(int i)
        {
            Console.WriteLine($"[{DateTime.Now.ToLongTimeString()}] Processing {i}");
            Thread.Sleep(new Random().Next(1000, 10000)); // 10 seconds
            Console.WriteLine($"[{DateTime.Now.ToLongTimeString()}] Finished processing {i}");
        }
    }
}
