using System;
using System.Threading.Tasks;

#if DISRUPTOR_V5

namespace Disruptor.Processing
{
    /// <summary>
    /// An event processor (<see cref="IEventProcessor"/>) for a reference-type ring buffer.
    /// </summary>
    /// <typeparam name="T">the type of event used.</typeparam>
    public interface IAsyncEventProcessor<T> : IEventProcessor
        where T : class
    {
        /// <summary>
        /// Asynchronously runs the processor.
        /// </summary>
        Task RunAsync();

        /// <summary>
        /// Waits before the event processor enters the <see cref="IEventProcessor.IsRunning"/> state.
        /// </summary>
        /// <param name="timeout">maximum wait duration</param>
        void WaitUntilStarted(TimeSpan timeout);

        /// <summary>
        /// Set a new <see cref="IExceptionHandler{T}"/> for handling exceptions propagated out of the <see cref="IEventHandler{T}"/>
        /// </summary>
        /// <param name="exceptionHandler">exceptionHandler to replace the existing exceptionHandler.</param>
        void SetExceptionHandler(IExceptionHandler<T> exceptionHandler);
    }
}

#endif