using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Disruptor.Processing;
using Disruptor.Tests.Support;
using NUnit.Framework;

namespace Disruptor.Tests.Processing;

[TestFixture]
public class AsyncBatchEventProcessorTests
{
    private readonly RingBuffer<StubEvent> _ringBuffer;
    private readonly AsyncSequenceBarrier _sequenceBarrier;

    public AsyncBatchEventProcessorTests()
    {
        _ringBuffer = new RingBuffer<StubEvent>(() => new StubEvent(-1), new MultiProducerSequencer(16, new AsyncWaitStrategy()));
        _sequenceBarrier = _ringBuffer.NewAsyncBarrier();
    }

    private static IAsyncEventProcessor<T> CreateEventProcessor<T>(IDataProvider<T> dataProvider, AsyncSequenceBarrier sequenceBarrier, IAsyncBatchEventHandler<T> eventHandler)
        where T : class
    {
        return EventProcessorFactory.Create(dataProvider, sequenceBarrier, eventHandler);
    }

    [Test]
    public void ShouldThrowExceptionOnSettingNullExceptionHandler()
    {
        var eventHandler = new TestAsyncBatchEventHandler<StubEvent>(x => throw new NullReferenceException());
        var eventProcessor = CreateEventProcessor(_ringBuffer, _sequenceBarrier, eventHandler);

        Assert.Throws<ArgumentNullException>(() => eventProcessor.SetExceptionHandler(null!));
    }

    [Test]
    public void ShouldCallMethodsInLifecycleOrderForBatch()
    {
        var eventSignal = new CountdownEvent(3);
        var eventHandler = new TestAsyncBatchEventHandler<StubEvent>(x => eventSignal.Signal());
        var eventProcessor = CreateEventProcessor(_ringBuffer, _sequenceBarrier, eventHandler);

        _ringBuffer.AddGatingSequences(eventProcessor.Sequence);

        _ringBuffer.PublishStubEvent(0);
        _ringBuffer.PublishStubEvent(0);
        _ringBuffer.PublishStubEvent(0);

        var task = eventProcessor.Start();

        Assert.IsTrue(eventSignal.Wait(TimeSpan.FromSeconds(2)));

        eventProcessor.Halt();

        Assert.IsTrue(task.Wait(TimeSpan.FromSeconds(2)));
    }

    [Test]
    public void ShouldCallOnTimeout()
    {
        var waitStrategy = new AsyncWaitStrategy(TimeSpan.FromMilliseconds(1));
        var ringBuffer = new RingBuffer<StubEvent>(() => new StubEvent(-1), new SingleProducerSequencer(16, waitStrategy));
        var sequenceBarrier = ringBuffer.NewAsyncBarrier();

        var onTimeoutSignal = new ManualResetEvent(false);
        var eventHandler = new TestAsyncBatchEventHandler<StubEvent> { OnTimeoutAction = () => onTimeoutSignal.Set() };
        var eventProcessor = CreateEventProcessor(ringBuffer, sequenceBarrier, eventHandler);
        ringBuffer.AddGatingSequences(eventProcessor.Sequence);

        var task = eventProcessor.Start();

        Assert.IsTrue(onTimeoutSignal.WaitOne(TimeSpan.FromSeconds(2)));

        eventProcessor.Halt();

        Assert.IsTrue(task.Wait(TimeSpan.FromSeconds(2)));
    }

    [Test]
    public void ShouldCallExceptionHandlerOnTimeoutException()
    {
        var waitStrategy = new AsyncWaitStrategy(TimeSpan.FromMilliseconds(1));
        var ringBuffer = new RingBuffer<StubEvent>(() => new StubEvent(-1), new SingleProducerSequencer(16, waitStrategy));
        var sequenceBarrier = ringBuffer.NewAsyncBarrier();

        var exception = new TaskCompletionSource<Exception>();
        var exceptionHandler = new TestExceptionHandler<StubEvent>(x => exception.TrySetResult(x.ex));
        var eventHandler = new TestAsyncBatchEventHandler<StubEvent> { OnTimeoutAction = TestException.ThrowOnce() };
        var eventProcessor = CreateEventProcessor(ringBuffer, sequenceBarrier, eventHandler);
        ringBuffer.AddGatingSequences(eventProcessor.Sequence);

        eventProcessor.SetExceptionHandler(exceptionHandler);

        var task = eventProcessor.Start();

        Assert.IsTrue(exception.Task.Wait(TimeSpan.FromSeconds(2)));
        Assert.AreEqual(0, exceptionHandler.EventExceptionCount);
        Assert.AreEqual(1, exceptionHandler.TimeoutExceptionCount);
        Assert.AreEqual(0, exceptionHandler.BatchExceptionCount);

        eventProcessor.Halt();

        Assert.IsTrue(task.Wait(TimeSpan.FromSeconds(2)));
    }

    [Test]
    public void ShouldCallExceptionHandlerOnUncaughtException()
    {
        var exceptionSignal = new CountdownEvent(1);
        var exceptionHandler = new TestExceptionHandler<StubEvent>(x => exceptionSignal.Signal());
        var eventHandler = new TestAsyncBatchEventHandler<StubEvent>(x => throw new NullReferenceException());
        var eventProcessor = CreateEventProcessor(_ringBuffer, _sequenceBarrier, eventHandler);
        _ringBuffer.AddGatingSequences(eventProcessor.Sequence);

        eventProcessor.SetExceptionHandler(exceptionHandler);

        var task = eventProcessor.Start();

        _ringBuffer.PublishStubEvent(0);

        Assert.IsTrue(exceptionSignal.Wait(TimeSpan.FromSeconds(2)));
        Assert.AreEqual(0, exceptionHandler.EventExceptionCount);
        Assert.AreEqual(0, exceptionHandler.TimeoutExceptionCount);
        Assert.AreEqual(1, exceptionHandler.BatchExceptionCount);

        eventProcessor.Halt();

        Assert.IsTrue(task.Wait(TimeSpan.FromSeconds(2)));
    }

    [Test]
    public void ShouldCallExceptionHandlerOnMultipleUncaughtException()
    {
        var processingSignal = new AutoResetEvent(false);
        var exceptionHandler = new TestExceptionHandler<StubEvent>(x => processingSignal.Set());
        var eventHandler = new TestAsyncBatchEventHandler<StubEvent>(x =>
        {
            if (x.Value == 1)
                throw new Exception();

            processingSignal.Set();
        });
        var eventProcessor = CreateEventProcessor(_ringBuffer, _sequenceBarrier, eventHandler);
        _ringBuffer.AddGatingSequences(eventProcessor.Sequence);

        eventProcessor.SetExceptionHandler(exceptionHandler);

        var task = eventProcessor.Start();

        _ringBuffer.PublishStubEvent(0);
        Assert.IsTrue(processingSignal.WaitOne(TimeSpan.FromSeconds(2)));
        Assert.AreEqual(0, exceptionHandler.EventExceptionCount);
        Assert.AreEqual(0, exceptionHandler.TimeoutExceptionCount);
        Assert.AreEqual(0, exceptionHandler.BatchExceptionCount);

        _ringBuffer.PublishStubEvent(1);
        Assert.IsTrue(processingSignal.WaitOne(TimeSpan.FromSeconds(2)));
        Assert.AreEqual(0, exceptionHandler.EventExceptionCount);
        Assert.AreEqual(0, exceptionHandler.TimeoutExceptionCount);
        Assert.AreEqual(1, exceptionHandler.BatchExceptionCount);

        _ringBuffer.PublishStubEvent(0);
        Assert.IsTrue(processingSignal.WaitOne(TimeSpan.FromSeconds(2)));
        Assert.AreEqual(0, exceptionHandler.EventExceptionCount);
        Assert.AreEqual(0, exceptionHandler.TimeoutExceptionCount);
        Assert.AreEqual(1, exceptionHandler.BatchExceptionCount);

        _ringBuffer.PublishStubEvent(1);
        Assert.IsTrue(processingSignal.WaitOne(TimeSpan.FromSeconds(2)));
        Assert.AreEqual(0, exceptionHandler.EventExceptionCount);
        Assert.AreEqual(0, exceptionHandler.TimeoutExceptionCount);
        Assert.AreEqual(2, exceptionHandler.BatchExceptionCount);

        _ringBuffer.PublishStubEvent(0);
        Assert.IsTrue(processingSignal.WaitOne(TimeSpan.FromSeconds(2)));
        Assert.AreEqual(0, exceptionHandler.EventExceptionCount);
        Assert.AreEqual(0, exceptionHandler.TimeoutExceptionCount);
        Assert.AreEqual(2, exceptionHandler.BatchExceptionCount);

        eventProcessor.Halt();

        Assert.IsTrue(task.Wait(TimeSpan.FromSeconds(2)));
    }

    [Test]
    public void ShouldAlwaysHalt()
    {
        var waitStrategy = new AsyncWaitStrategy();
        var sequencer = new SingleProducerSequencer(8, waitStrategy);
        var barrier = new AsyncSequenceBarrier(sequencer, waitStrategy, new DependentSequenceGroup(new Sequence()));
        var dp = new ArrayDataProvider<StubEvent>(sequencer.BufferSize);

        var h1 = new LifeCycleHandler();
        var p1 = CreateEventProcessor(dp, barrier, h1);

        p1.Halt();
        p1.Start();

        Assert.IsTrue(h1.WaitStart(TimeSpan.FromSeconds(2)));
        Assert.IsTrue(h1.WaitShutdown(TimeSpan.FromSeconds(2)));

        for (int i = 0; i < 1000; i++)
        {
            var h2 = new LifeCycleHandler();
            var p2 =  CreateEventProcessor(dp, barrier, h2);
            p2.Start();

            p2.Halt();

            Assert.IsTrue(h2.WaitStart(TimeSpan.FromSeconds(2)));
            Assert.IsTrue(h2.WaitShutdown(TimeSpan.FromSeconds(2)));
        }

        for (int i = 0; i < 1000; i++)
        {
            var h2 = new LifeCycleHandler();
            var p2 =  CreateEventProcessor(dp, barrier, h2);

            p2.Start();
            Thread.Yield();
            p2.Halt();

            Assert.IsTrue(h2.WaitStart(TimeSpan.FromSeconds(2)));
            Assert.IsTrue(h2.WaitShutdown(TimeSpan.FromSeconds(2)));
        }
    }

    [Test]
    public void ShouldInvokeOnStartAndOnShutdown()
    {
        var handler = new LifeCycleHandler();
        var processor = CreateEventProcessor(_ringBuffer, _sequenceBarrier, handler);

        var task = processor.Start();

        var wasStarted = handler.WaitStart(TimeSpan.FromMilliseconds(500));
        Assert.IsTrue(wasStarted);

        var wasShutdownAfterStart = handler.WaitShutdown(TimeSpan.FromMilliseconds(10));
        Assert.IsFalse(wasShutdownAfterStart);

        processor.Halt();

        var stopped = task.Wait(TimeSpan.FromMilliseconds(500));
        Assert.IsTrue(stopped);

        var wasShutdownAfterStop = handler.WaitShutdown(TimeSpan.FromMilliseconds(10));
        Assert.IsTrue(wasShutdownAfterStop);
    }

    [Test]
    public void ShouldLimitMaxBatchSize()
    {
        const int eventCountCount = 15;

        var eventSignal = new CountdownEvent(eventCountCount);
        var eventHandler = new LimitedBatchSizeEventHandler(6, eventSignal);

        var eventProcessor = CreateEventProcessor(_ringBuffer, _sequenceBarrier, eventHandler);
        _ringBuffer.AddGatingSequences(eventProcessor.Sequence);

        for (var i = 0; i < eventCountCount; i++)
        {
            _ringBuffer.PublishStubEvent(i);
        }

        eventProcessor.Start();

        Assert.IsTrue(eventSignal.Wait(TimeSpan.FromSeconds(2)));
        CollectionAssert.AreEqual(new[] { 6, 6, 3 }, eventHandler.BatchSizes);

        eventProcessor.Halt();
    }

    public class LimitedBatchSizeEventHandler : IAsyncBatchEventHandler<StubEvent>
    {
        private readonly CountdownEvent _countdownEvent;

        public LimitedBatchSizeEventHandler(int maxBatchSize, CountdownEvent countdownEvent)
        {
            MaxBatchSize = maxBatchSize;
            _countdownEvent = countdownEvent;
        }

        public int? MaxBatchSize { get; }
        public List<long> BatchSizes { get; } = new();

        public async ValueTask OnBatch(EventBatch<StubEvent> batch, long sequence)
        {
            await Task.Yield();

            BatchSizes.Add(batch.Length);

            _countdownEvent.Signal(batch.Length);
        }
    }

    private class LifeCycleHandler : IAsyncBatchEventHandler<StubEvent>
    {
        private readonly ManualResetEvent _startedSignal = new(false);
        private readonly ManualResetEvent _shutdownSignal = new(false);

        public async ValueTask OnBatch(EventBatch<StubEvent> batch, long sequence)
        {
            await Task.Yield();
        }

        public void OnStart()
        {
            _startedSignal.Set();
        }

        public void OnShutdown()
        {
            _shutdownSignal.Set();
        }

        public bool WaitStart(TimeSpan timeSpan)
        {
            return _startedSignal.WaitOne(timeSpan);
        }

        public bool WaitShutdown(TimeSpan timeSpan)
        {
            return _shutdownSignal.WaitOne(timeSpan);
        }
    }
}
