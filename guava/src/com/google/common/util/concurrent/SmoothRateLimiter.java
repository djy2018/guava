/*
 * Copyright (C) 2012 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.common.util.concurrent;

import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.annotations.GwtIncompatible;
import com.google.common.math.LongMath;

import java.util.concurrent.TimeUnit;

@GwtIncompatible
abstract class SmoothRateLimiter extends RateLimiter {
    /*
     * How is the RateLimiter designed, and why?
     *
     * The primary feature of a RateLimiter is its "stable rate", the maximum rate that it should
     * allow in normal conditions. This is enforced by "throttling" incoming requests as needed. For
     * example, we could compute the appropriate throttle time for an incoming request, and make the
     * calling thread wait for that time.
     *
     * The simplest way to maintain a rate of QPS is to keep the timestamp of the last granted
     * request, and ensure that (1/QPS) seconds have elapsed since then. For example, for a rate of
     * QPS=5 (5 tokens per second), if we ensure that a request isn't granted earlier than 200ms after
     * the last one, then we achieve the intended rate. If a request comes and the last request was
     * granted only 100ms ago, then we wait for another 100ms. At this rate, serving 15 fresh permits
     * (i.e. for an acquire(15) request) naturally takes 3 seconds.
     *
     * It is important to realize that such a RateLimiter has a very superficial memory of the past:
     * it only remembers the last request. What if the RateLimiter was unused for a long period of
     * time, then a request arrived and was immediately granted? This RateLimiter would immediately
     * forget about that past underutilization. This may result in either underutilization or
     * overflow, depending on the real world consequences of not using the expected rate.
     *
     * Past underutilization could mean that excess resources are available. Then, the RateLimiter
     * should speed up for a while, to take advantage of these resources. This is important when the
     * rate is applied to networking (limiting bandwidth), where past underutilization typically
     * translates to "almost empty buffers", which can be filled immediately.
     *
     * On the other hand, past underutilization could mean that "the server responsible for handling
     * the request has become less ready for future requests", i.e. its caches become stale, and
     * requests become more likely to trigger expensive operations (a more extreme case of this
     * example is when a server has just booted, and it is mostly busy with getting itself up to
     * speed).
     *
     * To deal with such scenarios, we add an extra dimension, that of "past underutilization",
     * modeled by "storedPermits" variable. This variable is zero when there is no underutilization,
     * and it can grow up to maxStoredPermits, for sufficiently large underutilization. So, the
     * requested permits, by an invocation acquire(permits), are served from:
     *
     * - stored permits (if available)
     *
     * - fresh permits (for any remaining permits)
     *
     * How this works is best explained with an example:
     *
     * For a RateLimiter that produces 1 token per second, every second that goes by with the
     * RateLimiter being unused, we increase storedPermits by 1. Say we leave the RateLimiter unused
     * for 10 seconds (i.e., we expected a request at time X, but we are at time X + 10 seconds before
     * a request actually arrives; this is also related to the point made in the last paragraph), thus
     * storedPermits becomes 10.0 (assuming maxStoredPermits >= 10.0). At that point, a request of
     * acquire(3) arrives. We serve this request out of storedPermits, and reduce that to 7.0 (how
     * this is translated to throttling time is discussed later). Immediately after, assume that an
     * acquire(10) request arriving. We serve the request partly from storedPermits, using all the
     * remaining 7.0 permits, and the remaining 3.0, we serve them by fresh permits produced by the
     * rate limiter.
     *
     * We already know how much time it takes to serve 3 fresh permits: if the rate is
     * "1 token per second", then this will take 3 seconds. But what does it mean to serve 7 stored
     * permits? As explained above, there is no unique answer. If we are primarily interested to deal
     * with underutilization, then we want stored permits to be given out /faster/ than fresh ones,
     * because underutilization = free resources for the taking. If we are primarily interested to
     * deal with overflow, then stored permits could be given out /slower/ than fresh ones. Thus, we
     * require a (different in each case) function that translates storedPermits to throttling time.
     *
     * This role is played by storedPermitsToWaitTime(double storedPermits, double permitsToTake). The
     * underlying model is a continuous function mapping storedPermits (from 0.0 to maxStoredPermits)
     * onto the 1/rate (i.e. intervals) that is effective at the given storedPermits. "storedPermits"
     * essentially measure unused time; we spend unused time buying/storing permits. Rate is
     * "permits / time", thus "1 / rate = time / permits". Thus, "1/rate" (time / permits) times
     * "permits" gives time, i.e., integrals on this function (which is what storedPermitsToWaitTime()
     * computes) correspond to minimum intervals between subsequent requests, for the specified number
     * of requested permits.
     *
     * Here is an example of storedPermitsToWaitTime: If storedPermits == 10.0, and we want 3 permits,
     * we take them from storedPermits, reducing them to 7.0, and compute the throttling for these as
     * a call to storedPermitsToWaitTime(storedPermits = 10.0, permitsToTake = 3.0), which will
     * evaluate the integral of the function from 7.0 to 10.0.
     *
     * Using integrals guarantees that the effect of a single acquire(3) is equivalent to {
     * acquire(1); acquire(1); acquire(1); }, or { acquire(2); acquire(1); }, etc, since the integral
     * of the function in [7.0, 10.0] is equivalent to the sum of the integrals of [7.0, 8.0], [8.0,
     * 9.0], [9.0, 10.0] (and so on), no matter what the function is. This guarantees that we handle
     * correctly requests of varying weight (permits), /no matter/ what the actual function is - so we
     * can tweak the latter freely. (The only requirement, obviously, is that we can compute its
     * integrals).
     *
     * Note well that if, for this function, we chose a horizontal line, at height of exactly (1/QPS),
     * then the effect of the function is non-existent: we serve storedPermits at exactly the same
     * cost as fresh ones (1/QPS is the cost for each). We use this trick later.
     *
     * If we pick a function that goes /below/ that horizontal line, it means that we reduce the area
     * of the function, thus time. Thus, the RateLimiter becomes /faster/ after a period of
     * underutilization. If, on the other hand, we pick a function that goes /above/ that horizontal
     * line, then it means that the area (time) is increased, thus storedPermits are more costly than
     * fresh permits, thus the RateLimiter becomes /slower/ after a period of underutilization.
     *
     * Last, but not least: consider a RateLimiter with rate of 1 permit per second, currently
     * completely unused, and an expensive acquire(100) request comes. It would be nonsensical to just
     * wait for 100 seconds, and /then/ start the actual task. Why wait without doing anything? A much
     * better approach is to /allow/ the request right away (as if it was an acquire(1) request
     * instead), and postpone /subsequent/ requests as needed. In this version, we allow starting the
     * task immediately, and postpone by 100 seconds future requests, thus we allow for work to get
     * done in the meantime instead of waiting idly.
     *
     * This has important consequences: it means that the RateLimiter doesn't remember the time of the
     * _last_ request, but it remembers the (expected) time of the _next_ request. This also enables
     * us to tell immediately (see tryAcquire(timeout)) whether a particular timeout is enough to get
     * us to the point of the next scheduling time, since we always maintain that. And what we mean by
     * "an unused RateLimiter" is also defined by that notion: when we observe that the
     * "expected arrival time of the next request" is actually in the past, then the difference (now -
     * past) is the amount of time that the RateLimiter was formally unused, and it is that amount of
     * time which we translate to storedPermits. (We increase storedPermits with the amount of permits
     * that would have been produced in that idle time). So, if rate == 1 permit per second, and
     * arrivals come exactly one second after the previous, then storedPermits is _never_ increased --
     * we would only increase it for arrivals _later_ than the expected one second.
     */

    /**
     * This implements the following function where coldInterval = coldFactor * stableInterval.
     *
     * <pre>
     *          ^ throttling
     *          |
     *    cold  +                  /
     * interval |                 /.
     *          |                / .
     *          |               /  .   ← "warmup period" is the area of the trapezoid between
     *          |              /   .     thresholdPermits and maxPermits
     *          |             /    .
     *          |            /     .
     *          |           /      .
     *   stable +----------/  WARM .
     * interval |          .   UP  .
     *          |          . PERIOD.
     *          |          .       .
     *        0 +----------+-------+--------------→ storedPermits
     *          0 thresholdPermits maxPermits
     * </pre>
     * <p>
     * Before going into the details of this particular function, let's keep in mind the basics:
     *
     * <ol>
     *   <li>The state of the RateLimiter (storedPermits) is a vertical line in this figure.
     *   <li>When the RateLimiter is not used, this goes right (up to maxPermits)
     *   <li>When the RateLimiter is used, this goes left (down to zero), since if we have
     *       storedPermits, we serve from those first
     *   <li>When _unused_, we go right at a constant rate! The rate at which we move to the right is
     *       chosen as maxPermits / warmupPeriod. This ensures that the time it takes to go from 0 to
     *       maxPermits is equal to warmupPeriod.
     *   <li>When _used_, the time it takes, as explained in the introductory class note, is equal to
     *       the integral of our function, between X permits and X-K permits, assuming we want to
     *       spend K saved permits.
     * </ol>
     *
     * <p>In summary, the time it takes to move to the left (spend K permits), is equal to the area of
     * the function of width == K.
     *
     * <p>Assuming we have saturated demand, the time to go from maxPermits to thresholdPermits is
     * equal to warmupPeriod. And the time to go from thresholdPermits to 0 is warmupPeriod/2. (The
     * reason that this is warmupPeriod/2 is to maintain the behavior of the original implementation
     * where coldFactor was hard coded as 3.)
     *
     * <p>It remains to calculate thresholdsPermits and maxPermits.
     *
     * <ul>
     *   <li>The time to go from thresholdPermits to 0 is equal to the integral of the function
     *       between 0 and thresholdPermits. This is thresholdPermits * stableIntervals. By (5) it is
     *       also equal to warmupPeriod/2. Therefore
     *       <blockquote>
     *       thresholdPermits = 0.5 * warmupPeriod / stableInterval
     *       </blockquote>
     *   <li>The time to go from maxPermits to thresholdPermits is equal to the integral of the
     *       function between thresholdPermits and maxPermits. This is the area of the pictured
     *       trapezoid, and it is equal to 0.5 * (stableInterval + coldInterval) * (maxPermits -
     *       thresholdPermits). It is also equal to warmupPeriod, so
     *       <blockquote>
     *       maxPermits = thresholdPermits + 2 * warmupPeriod / (stableInterval + coldInterval)
     *       </blockquote>
     * </ul>
     */
    static final class SmoothWarmingUp extends SmoothRateLimiter {
        private final long warmupPeriodMicros;
        /**
         * The slope of the line from the stable interval (when permits == 0), to the cold interval
         * (when permits == maxPermits)
         */
        private double slope;

        private double thresholdPermits;
        private double coldFactor;

        SmoothWarmingUp(
                SleepingStopwatch stopwatch, long warmupPeriod, TimeUnit timeUnit, double coldFactor) {
            super(stopwatch);
            this.warmupPeriodMicros = timeUnit.toMicros(warmupPeriod);
            this.coldFactor = coldFactor;
        }

        @Override
        void doSetRate(double permitsPerSecond, double stableIntervalMicros) {
            double oldMaxPermits = maxPermits;
            // coldFactor 固定为3.0
            double coldIntervalMicros = stableIntervalMicros * coldFactor;
            // 有一个关键点：从 thresholdPermits 到 0 的时间，是从 maxPermits 到 thresholdPermits 时间的一半，
            // 也就是梯形的面积是长方形面积的 2 倍，梯形的面积是 warmupPeriod。
            thresholdPermits = 0.5 * warmupPeriodMicros / stableIntervalMicros;
            maxPermits =
                    thresholdPermits + 2.0 * warmupPeriodMicros / (stableIntervalMicros + coldIntervalMicros);
            // 计算斜线的斜率
            slope = (coldIntervalMicros - stableIntervalMicros) / (maxPermits - thresholdPermits);

            if (oldMaxPermits == Double.POSITIVE_INFINITY) {
                // if we don't special-case this, we would get storedPermits == NaN, below
                storedPermits = 0.0;
            } else {
                storedPermits =
                        (oldMaxPermits == 0.0)
                                ? maxPermits // initial state is cold
                                : storedPermits * maxPermits / oldMaxPermits;
            }
        }

        @Override
        long storedPermitsToWaitTime(double storedPermits, double permitsToTake) {
            // 可用的 permits 数量
            double availablePermitsAboveThreshold = storedPermits - thresholdPermits;
            long micros = 0;
            // measuring the integral on the right part of the function (the climbing line)
            if (availablePermitsAboveThreshold > 0.0) {
                // 从右边部分获取的 permits 数量
                double permitsAboveThresholdToTake = min(availablePermitsAboveThreshold, permitsToTake);
                // TODO(cpovirk): Figure out a good name for this variable.
                double length =
                        permitsToTime(availablePermitsAboveThreshold)
                                + permitsToTime(availablePermitsAboveThreshold - permitsAboveThresholdToTake);
                micros = (long) (permitsAboveThresholdToTake * length / 2.0);
                permitsToTake -= permitsAboveThresholdToTake;
            }
            // measuring the integral on the left part of the function (the horizontal line)
            // 加上长方形部分的阴影面积
            micros += (long) (stableIntervalMicros * permitsToTake);
            return micros;
        }

        /**
         * 对于给定的 x 值，计算 y 值
         *
         * @param permits x轴对应值
         * @return
         */
        private double permitsToTime(double permits) {
            return stableIntervalMicros + permits * slope;
        }

        @Override
        double coolDownIntervalMicros() {
            // 冷却时间间隔，它指的是 storedPermits 中每个 permit 的增长速度，也就是我们前面说的 x=k 这条垂直线
            // 往右的移动速度，为了达到从 0 到 maxPermits 花费 warmupPeriodMicros 的时间。
            return warmupPeriodMicros / maxPermits;
        }
    }

    /**
     * This implements a "bursty" RateLimiter, where storedPermits are translated to zero throttling.
     * The maximum number of permits that can be saved (when the RateLimiter is unused) is defined in
     * terms of time, in this sense: if a RateLimiter is 2qps, and this time is specified as 10
     * seconds, we can save up to 2 * 10 = 20 permits.
     */
    static final class SmoothBursty extends SmoothRateLimiter {
        /**
         * The work (permits) of how many seconds can be saved up if this RateLimiter is unused?
         */
        final double maxBurstSeconds;

        SmoothBursty(SleepingStopwatch stopwatch, double maxBurstSeconds) {
            super(stopwatch);
            this.maxBurstSeconds = maxBurstSeconds;
        }

        @Override
        void doSetRate(double permitsPerSecond, double stableIntervalMicros) {
            double oldMaxPermits = this.maxPermits;
            maxPermits = maxBurstSeconds * permitsPerSecond;
            if (oldMaxPermits == Double.POSITIVE_INFINITY) {
                // if we don't special-case this, we would get storedPermits == NaN, below
                storedPermits = maxPermits;
            } else {
                /*
                 * 兼容当 permitsPerSecond 速率发生变化时，storedPermits 需要等比例缩放。
                 * 比如：permitsPerSecond 由 2/s -> 4/s，此时 maxPermits 重新计算为4个 ，
                 * 如果此时 storedPermits=1个 oldMaxPermits=2个，那么需要将 storedPermits 放大到 2个。
                 */
                storedPermits =
                        (oldMaxPermits == 0.0)
                                ? 0.0 // initial state
                                : storedPermits * maxPermits / oldMaxPermits;
            }
        }

        @Override
        long storedPermitsToWaitTime(double storedPermits, double permitsToTake) {
            return 0L;
        }

        @Override
        double coolDownIntervalMicros() {
            return stableIntervalMicros;
        }
    }

    /**
     * The currently stored permits.
     */
    double storedPermits;

    /**
     * The maximum number of stored permits.
     */
    double maxPermits;

    /**
     * The interval between two unit requests, at our stable rate. E.g., a stable rate of 5 permits
     * per second has a stable interval of 200ms.
     */
    double stableIntervalMicros;

    /**
     * The time when the next request (no matter its size) will be granted. After granting a request,
     * this is pushed further in the future. Large requests push this further than small requests.
     */
    private long nextFreeTicketMicros = 0L; // could be either in the past or future

    private SmoothRateLimiter(SleepingStopwatch stopwatch) {
        super(stopwatch);
    }

    @Override
    final void doSetRate(double permitsPerSecond, long nowMicros) {
        // 同步
        resync(nowMicros);
        // 计算属性 stableIntervalMicros
        double stableIntervalMicros = SECONDS.toMicros(1L) / permitsPerSecond;
        this.stableIntervalMicros = stableIntervalMicros;
        doSetRate(permitsPerSecond, stableIntervalMicros);
    }

    abstract void doSetRate(double permitsPerSecond, double stableIntervalMicros);

    @Override
    final double doGetRate() {
        return SECONDS.toMicros(1L) / stableIntervalMicros;
    }

    @Override
    final long queryEarliestAvailable(long nowMicros) {
        return nextFreeTicketMicros;
    }

    /**
     * 获取 permits 的时候，permits分为2个部分：
     * 1) 来自于存量 storedPermits 2) 针对情况1存量不够情况下，来自于预占未来的 freshPermits
     * 关键点：
     * 返回值是：nextFreeTicketMicros 的旧值，因为只要到这个时间点，就说明当前调用 acquire 可以成功反悔了，
     * 而不管 storedPermits 够不够。如果 storedPermits 不够，会将 nextFreeTicketMicros 往前推一定的时间，
     * 预占了一定的量。
     *
     * @param requiredPermits 需要的 permit 数量
     * @param nowMicros       当前时间
     * @return
     */
    @Override
    final long reserveEarliestAvailable(int requiredPermits, long nowMicros) {
        // 同步，更新 storedPermits 和 nextFreeTicketMicros(如果需要)
        resync(nowMicros);
        // 返回值就是 nextFreeTicketMicros，注意
        long returnValue = nextFreeTicketMicros;
        // storedPermits 中可以使用多少个 permits
        double storedPermitsToSpend = min(requiredPermits, this.storedPermits);
        // storedPermits 中不够的部分
        double freshPermits = requiredPermits - storedPermitsToSpend;
        // 如果 permits 不够，需要等待多久
        long waitMicros =
                storedPermitsToWaitTime(this.storedPermits, storedPermitsToSpend) // 这部分固定返回0
                        + (long) (freshPermits * stableIntervalMicros);
        // 把 nextFreeTicketMicros 往前推
        this.nextFreeTicketMicros = LongMath.saturatedAdd(nextFreeTicketMicros, waitMicros);
        // storedPermits 减去被拿走的部分
        this.storedPermits -= storedPermitsToSpend;
        return returnValue;
    }

    /**
     * Translates a specified portion of our currently stored permits which we want to spend/acquire,
     * into a throttling time. Conceptually, this evaluates the integral of the underlying function we
     * use, for the range of [(storedPermits - permitsToTake), storedPermits].
     *
     * <p>This always holds: {@code 0 <= permitsToTake <= storedPermits}
     */
    abstract long storedPermitsToWaitTime(double storedPermits, double permitsToTake);

    /**
     * Returns the number of microseconds during cool down that we have to wait to get a new permit.
     */
    abstract double coolDownIntervalMicros();

    /**
     * Updates {@code storedPermits} and {@code nextFreeTicketMicros} based on the current time.
     */
    void resync(long nowMicros) {
        // if nextFreeTicket is in the past, resync to now
        if (nowMicros > nextFreeTicketMicros) {
            double newPermits = (nowMicros - nextFreeTicketMicros) / coolDownIntervalMicros();
            storedPermits = min(maxPermits, storedPermits + newPermits);
            nextFreeTicketMicros = nowMicros;
        }
    }
}
