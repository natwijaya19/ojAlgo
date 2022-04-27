/*
 * Copyright 1997-2022 Optimatika
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.ojalgo.data.batch;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.ojalgo.concurrent.Parallelism;
import org.ojalgo.concurrent.ProcessingService;
import org.ojalgo.function.special.PowerOf2;
import org.ojalgo.netio.DataInterpreter;
import org.ojalgo.netio.DataReader;
import org.ojalgo.netio.DataWriter;
import org.ojalgo.netio.ShardedFile;
import org.ojalgo.netio.ToFileWriter;
import org.ojalgo.type.function.TwoStepMapper;

/**
 */
public final class BatchNode<T> {

    public static final class Builder<T> {

        private int myCapacity = 1024;
        private final File myDirectory;
        private ToIntFunction<T> myDistributor = obj -> ThreadLocalRandom.current().nextInt();
        private ExecutorService myExecutor = null;
        private int myFragmentation = 64;
        private final DataInterpreter<T> myInterpreter;

        private IntSupplier myParallelism = Parallelism.CORES;

        Builder(final File directory, final DataInterpreter<T> interpreter) {
            super();
            myDirectory = directory;
            myInterpreter = interpreter;
        }

        public BatchNode<T> build() {
            return new BatchNode<>(this);
        }

        public Builder<T> capacity(final int capacity) {
            myCapacity = capacity;
            return this;
        }

        public BatchNode.Builder<T> distributor(final ToIntFunction<T> distributor) {
            myDistributor = distributor;
            return this;
        }

        public BatchNode.Builder<T> executor(final ExecutorService executor) {
            myExecutor = executor;
            return this;
        }

        /**
         * The number of underlying files/shards. Increasing the fragmentation (the number of shards)
         * typically reduces memory requirements when processong. The value set here is only an indication of
         * the desired order of magnitude. The exact number of shards actually used is a derived property.
         */
        public BatchNode.Builder<T> fragmentation(final int fragmentation) {
            myFragmentation = fragmentation;
            return this;
        }

        public BatchNode.Builder<T> parallelism(final IntSupplier parallelism) {
            myParallelism = parallelism;
            return this;
        }

        int getCapacity() {
            return myCapacity;
        }

        ToIntFunction<T> getDistributor() {
            return myDistributor;
        }

        /**
         * The total number of files/shards. Will always be power of 2 as well as a multiple of
         * {@link #getParallelism()}.
         */
        int getFragmentation() {

            int parallelism = this.getParallelism().getAsInt(); // Is power of 2

            int factor = PowerOf2.adjustUp(Math.max(parallelism, myFragmentation) / parallelism);

            return factor * parallelism; // Is power of 2, and multiple of parallelism
        }

        DataInterpreter<T> getInterpreter() {
            return myInterpreter;
        }

        /**
         * Will always be power of 2
         */
        IntSupplier getParallelism() {
            return () -> PowerOf2.adjustDown(Math.min(myParallelism.getAsInt(), myFragmentation));
        }

        ProcessingService getProcessor() {
            if (myExecutor != null) {
                return new ProcessingService(myExecutor);
            } else {
                return ProcessingService.newInstance("BatchNode-" + myDirectory.getName());
            }
        }

        /**
         * There are "parallelism" number of partitions and "fragmentation" number of files in total.
         */
        ShardedFile getShardedFile() {
            return ShardedFile.of(myDirectory, "Part.data", this.getFragmentation());
        }

    }

    public static <T> BatchNode.Builder<T> newBuilder(final File directory, final DataInterpreter<T> interpreter) {
        return new BatchNode.Builder<>(directory, interpreter);
    }

    public static <T> BatchNode<T> newInstance(final File directory, final DataInterpreter<T> interpreter) {
        return new BatchNode<>(new BatchNode.Builder<>(directory, interpreter));
    }

    private final int myCapacity;
    private final ToIntFunction<T> myDistributor;
    private final DataInterpreter<T> myInterpreter;
    private final IntSupplier myParallelism;
    private final ProcessingService myProcessor;
    private final ShardedFile myShards;

    BatchNode(final BatchNode.Builder<T> builder) {

        super();

        myShards = builder.getShardedFile();
        myParallelism = builder.getParallelism();

        myInterpreter = builder.getInterpreter();
        myDistributor = builder.getDistributor();
        myProcessor = builder.getProcessor();
        myCapacity = builder.getCapacity();
    }

    /**
     * Explicitly delete all files
     */
    public void delete() {
        myShards.delete();
    }

    public ToFileWriter<T> newWriter() {

        int parallelism = myParallelism.getAsInt();
        int shardsPerThread = myShards.numberOfShards / parallelism;
        while (parallelism >= shardsPerThread) {
            parallelism /= 2;
            shardsPerThread = myShards.numberOfShards / parallelism;
        }

        return ToFileWriter.of(myShards).capacity(myCapacity).parallelism(parallelism).build(myDistributor, shard -> DataWriter.of(shard, myInterpreter));
    }

    /**
     * Process each and every item individually
     *
     * @param consumer Must be able to consume concurrently
     */
    public void processAll(final Consumer<T> consumer) {
        myProcessor.process(myShards.files(), myParallelism, shard -> this.process(shard, consumer));
    }

    /**
     * Process mapped/derived data in batches.
     * <P>
     * There will be one {@link TwoStepMapper} instance per underlying file/shard – that's a batch. Those
     * instances are likely to contain some sort of {@link Collection} or {@link Map} that hold mapped/derived
     * data.
     * <p>
     * You must make sure that all data items that need to be in the same {@link TwoStepMapper} instance (in
     * the same batch) are in the same file/shard. You control the number of shards via
     * {@link Builder#fragmentation(int)} and which item goes in which shard via
     * {@link Builder#distributor(ToIntFunction)}.
     *
     * @param <H> The mapped/derived data holding type
     * @param mapper Produces the {@link TwoStepMapper} mapping instances
     * @param consumer Consumes the mapped/derived data - one whole {@link TwoStepMapper} instance at the time
     */
    public <H> void processMapped(final Supplier<TwoStepMapper<T, H>> mapper, final Consumer<H> consumer) {
        ThreadLocal<TwoStepMapper<T, H>> threadLocal = ThreadLocal.withInitial(mapper);
        myProcessor.process(myShards.files(), myParallelism, shard -> this.process(shard, threadLocal::get, consumer));
    }

    /**
     * Same as {@link #processMapped(Supplier, Consumer)}, but then also reduce/merge the total results using
     * {@link TwoStepMapper#merge(Object)}.
     * <P>
     * Create a class that implements {@link TwoStepMapper} and make sure to also implement
     * {@link TwoStepMapper#merge(Object)} - you can only use this if merging partial (sub)results is
     * possible. Use a constructor or factory method that produce instances of that type as the argument to
     * this method.
     */
    public <R> R reduceMapped(final Supplier<TwoStepMapper<T, R>> mapper) {

        TwoStepMapper<T, R> totalResults = mapper.get();

        this.processMapped(mapper, totalResults::merge);

        return totalResults.getResults();
    }

    private DataReader<T> newReader(final File file) {
        return DataReader.of(file, myInterpreter);
    }

    private void process(final File shard, final Consumer<T> consumer) {

        try (DataReader<T> reader = this.newReader(shard)) {

            T item = null;
            while ((item = reader.read()) != null) {
                consumer.accept(item);
            }

        } catch (Exception cause) {
            throw new RuntimeException(cause);
        }

    }

    private <G> void process(final File shard, final Supplier<TwoStepMapper<T, G>> aggregatorSupplier, final Consumer<G> consumer) {

        TwoStepMapper<T, G> aggregator = aggregatorSupplier.get(); // It's a ThreadLocal...

        try (DataReader<T> reader = this.newReader(shard)) {

            T item = null;
            while ((item = reader.read()) != null) {
                aggregator.consume(item);
            }

            consumer.accept(aggregator.getResults());

            aggregator.reset(); // ...and needs to be reset.

        } catch (Exception cause) {
            throw new RuntimeException(cause);
        }
    }

}
