﻿using System.Threading;
using Disruptor.Processing;

namespace Disruptor
{
    /// <summary>
    /// Busy Spin strategy that uses a busy spin loop for <see cref="IEventProcessor"/>s waiting on a barrier.
    ///
    /// This strategy will use CPU resource to avoid syscalls which can introduce latency jitter.  It is best
    /// used when threads can be bound to specific CPU cores.
    /// </summary>
    public sealed class BusySpinWaitStrategy : INonBlockingWaitStrategy
    {
        /// <summary>
        /// <see cref="IWaitStrategy.WaitFor"/>
        /// </summary>
        public long WaitFor(long sequence, Sequence cursor, ISequence dependentSequence, CancellationToken cancellationToken)
        {
            long availableSequence;

            while ((availableSequence = dependentSequence.Value) < sequence)
            {
                cancellationToken.ThrowIfCancellationRequested();
            }

            return availableSequence;
        }

        /// <summary>
        /// <see cref="IWaitStrategy.SignalAllWhenBlocking"/>
        /// </summary>
        public void SignalAllWhenBlocking()
        {
        }
    }
}
