using Disruptor.PerfTests.Support;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Disruptor.PerfTests.Throughput.ThreeToOne;

public class ThreeToOneChannelThroughputTest : IThroughputTest, IExternalTest {
    private const int _publisherCount = 3;
    private const int _bufferSize = 1024 * 64;
    private const long _iterations = 1000 * 1000 * 20;
    private readonly Barrier _signal = new(_publisherCount + 1);
    private readonly Channel<long> _channel = Channel.CreateUnbounded<long>(new UnboundedChannelOptions() {
        SingleReader = true,
        SingleWriter = false
    }
    );
    private readonly PerfAdditionChannelEventProcessor _queueProcessor;
    private readonly PerfChannelProducer[] _perfQueueProducers = new PerfChannelProducer[_publisherCount];

    public ThreeToOneChannelThroughputTest() {
        _queueProcessor = new PerfAdditionChannelEventProcessor(_channel, (_iterations / _publisherCount * _publisherCount) - 1L);
        for (var i = 0; i < _publisherCount; i++) {
            _perfQueueProducers[i] = new PerfChannelProducer(_signal, _channel, _iterations / _publisherCount);
        }
    }

    public int RequiredProcessorCount => 4;

    public long Run(ThroughputSessionContext sessionContext) {
        var signal = new ManualResetEvent(false);
        _queueProcessor.Reset(signal);

        var tasks = new Task[_publisherCount];
        for (var i = 0; i < _publisherCount; i++) {
            tasks[i] = _perfQueueProducers[i].Start();
        }

        var processorTask = _queueProcessor.Start();

        sessionContext.Start();
        _signal.SignalAndWait();
        Task.WaitAll(tasks);
        signal.WaitOne();
        sessionContext.Stop();
        _queueProcessor.Halt();
        processorTask.Wait();

        return _iterations;
    }
}
