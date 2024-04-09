using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Disruptor.PerfTests.Support;

public class PerfChannelProducer {
    private readonly Barrier _barrier;
    private readonly Channel<long> _channel;
    private readonly long _iterations;

    public PerfChannelProducer(Barrier barrier, Channel<long> channel, long iterations) {
        _barrier = barrier;
        _channel = channel;
        _iterations = iterations;
    }

    public void Run() {
        _barrier.SignalAndWait();
        for (long i = 0; i < _iterations; i++) {
            _channel.Writer.TryWrite(i);
        }
    }

    public Task Start() {
        return PerfTestUtil.StartLongRunning(Run);
    }
}
