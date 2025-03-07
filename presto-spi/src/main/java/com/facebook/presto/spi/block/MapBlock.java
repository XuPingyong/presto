/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.spi.block;

import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.BiConsumer;

import static com.facebook.presto.spi.block.MapBlockBuilder.buildHashTable;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MapBlock
        extends AbstractMapBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapBlock.class).instanceSize();

    private final int startOffset;
    private final int positionCount;

    private final boolean[] mapIsNull;
    private final int[] offsets;
    private final Block keyBlock;
    private final Block valueBlock;
    private HashTables hashTables;

    private volatile long sizeInBytes;
    private final long retainedSizeInBytes;

    /**
     * Create a map block directly from columnar nulls, keys, values, and offsets into the keys and values.
     * A null map must have no entries.
     *
     * @param mapType key type K
     * @param keyBlockNativeEquals equality between key stack type and a block+position; signature is (K, Block, int)boolean
     * @param keyNativeHashCode hash of a key stack type; signature is (K)long
     */
    public static MapBlock fromKeyValueBlock(
            int positionCount,
            Optional<boolean[]> mapIsNull,
            int[] offsets,
            Block keyBlock,
            Block valueBlock,
            MapType mapType,
            MethodHandle keyBlockNativeEquals,
            MethodHandle keyNativeHashCode,
            MethodHandle keyBlockHashCode)
    {
        validateConstructorArguments(0, positionCount, mapIsNull.orElse(null), offsets, keyBlock, valueBlock, mapType.getKeyType(), keyBlockNativeEquals, keyNativeHashCode);

        return createMapBlockInternal(
                0,
                positionCount,
                mapIsNull,
                offsets,
                keyBlock,
                valueBlock,
                new HashTables(Optional.empty(), keyBlock.getPositionCount() * HASH_MULTIPLIER),
                mapType.getKeyType(),
                keyBlockNativeEquals,
                keyNativeHashCode,
                keyBlockHashCode);
    }

    /**
     * Create a map block directly without per element validations.
     * <p>
     * Internal use by this package and com.facebook.presto.spi.Type only.
     *
     * @param keyType key type K
     * @param keyBlockNativeEquals equality between key stack type and a block+position; signature is (K, Block, int)boolean
     * @param keyNativeHashCode hash of a key stack type; signature is (K)long
     */
    public static MapBlock createMapBlockInternal(
            int startOffset,
            int positionCount,
            Optional<boolean[]> mapIsNull,
            int[] offsets,
            Block keyBlock,
            Block valueBlock,
            HashTables hashTables,
            Type keyType,
            MethodHandle keyBlockNativeEquals,
            MethodHandle keyNativeHashCode,
            MethodHandle keyBlockHashCode)
    {
        validateConstructorArguments(startOffset, positionCount, mapIsNull.orElse(null), offsets, keyBlock, valueBlock, keyType, keyBlockNativeEquals, keyNativeHashCode);
        requireNonNull(hashTables, "hashTables is null");
        return new MapBlock(
                startOffset,
                positionCount,
                mapIsNull.orElse(null),
                offsets,
                keyBlock,
                valueBlock,
                hashTables,
                keyType,
                keyBlockNativeEquals,
                keyNativeHashCode,
                keyBlockHashCode);
    }

    private static void validateConstructorArguments(
            int startOffset,
            int positionCount,
            @Nullable boolean[] mapIsNull,
            int[] offsets,
            Block keyBlock,
            Block valueBlock,
            Type keyType,
            MethodHandle keyBlockNativeEquals,
            MethodHandle keyNativeHashCode)
    {
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset is negative");
        }

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }

        if (mapIsNull != null && mapIsNull.length - startOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }

        requireNonNull(offsets, "offsets is null");
        if (offsets.length - startOffset < positionCount + 1) {
            throw new IllegalArgumentException("offsets length is less than positionCount");
        }

        requireNonNull(keyBlock, "keyBlock is null");
        requireNonNull(valueBlock, "valueBlock is null");
        if (keyBlock.getPositionCount() != valueBlock.getPositionCount()) {
            throw new IllegalArgumentException(format("keyBlock and valueBlock has different size: %s %s", keyBlock.getPositionCount(), valueBlock.getPositionCount()));
        }

        requireNonNull(keyType, "keyType is null");
        requireNonNull(keyBlockNativeEquals, "keyBlockNativeEquals is null");
        requireNonNull(keyNativeHashCode, "keyNativeHashCode is null");
    }

    /**
     * Use createRowBlockInternal or fromKeyValueBlock instead of this method.  The caller of this method is assumed to have
     * validated the arguments with validateConstructorArguments.
     */
    private MapBlock(
            int startOffset,
            int positionCount,
            @Nullable boolean[] mapIsNull,
            int[] offsets,
            Block keyBlock,
            Block valueBlock,
            HashTables hashTables,
            Type keyType,
            MethodHandle keyBlockNativeEquals,
            MethodHandle keyNativeHashCode,
            MethodHandle keyBlockHashCode)
    {
        super(keyType, keyNativeHashCode, keyBlockNativeEquals, keyBlockHashCode);

        int[] rawHashTables = hashTables.get().orElse(null);
        if (rawHashTables != null && rawHashTables.length < keyBlock.getPositionCount() * HASH_MULTIPLIER) {
            throw new IllegalArgumentException(format("keyBlock/valueBlock size does not match hash table size: %s %s", keyBlock.getPositionCount(), rawHashTables.length));
        }

        this.startOffset = startOffset;
        this.positionCount = positionCount;
        this.mapIsNull = mapIsNull;
        this.offsets = offsets;
        this.keyBlock = keyBlock;
        this.valueBlock = valueBlock;
        this.hashTables = hashTables;
        this.sizeInBytes = -1;

        // We will add the hashtable size to the retained size even if it's not built yet. This could be overestimating
        // but is necessary to avoid reliability issues. Currently the memory counting framework only pull the retained
        // size once for each operator so updating in the middle of the processing would not work.
        this.retainedSizeInBytes = INSTANCE_SIZE
                + keyBlock.getRetainedSizeInBytes()
                + valueBlock.getRetainedSizeInBytes()
                + sizeOf(offsets)
                + sizeOf(mapIsNull)
                + hashTables.getRetainedSizeInBytes();
    }

    @Override
    protected Block getRawKeyBlock()
    {
        return keyBlock;
    }

    @Override
    protected Block getRawValueBlock()
    {
        return valueBlock;
    }

    @Override
    protected HashTables getHashTables()
    {
        return hashTables;
    }

    @Override
    protected int[] getOffsets()
    {
        return offsets;
    }

    @Override
    public int getOffsetBase()
    {
        return startOffset;
    }

    @Override
    @Nullable
    protected boolean[] getMapIsNull()
    {
        return mapIsNull;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        if (sizeInBytes < 0) {
            calculateSize();
        }
        return sizeInBytes;
    }

    private void calculateSize()
    {
        int entriesStart = offsets[startOffset];
        int entriesEnd = offsets[startOffset + positionCount];
        int entryCount = entriesEnd - entriesStart;
        sizeInBytes = keyBlock.getRegionSizeInBytes(entriesStart, entryCount) +
                valueBlock.getRegionSizeInBytes(entriesStart, entryCount) +
                (Integer.BYTES + Byte.BYTES) * (long) this.positionCount +
                Integer.BYTES * HASH_MULTIPLIER * (long) entryCount +
                hashTables.getInstanceSizeInBytes();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(keyBlock, keyBlock.getRetainedSizeInBytes());
        consumer.accept(valueBlock, valueBlock.getRetainedSizeInBytes());
        consumer.accept(offsets, sizeOf(offsets));
        consumer.accept(mapIsNull, sizeOf(mapIsNull));
        consumer.accept(hashTables, hashTables.getRetainedSizeInBytes());
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("MapBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    @Override
    public Block getLoadedBlock()
    {
        if (keyBlock != keyBlock.getLoadedBlock()) {
            // keyBlock has to be loaded since MapBlock constructs hash table eagerly.
            throw new IllegalStateException();
        }

        Block loadedValueBlock = valueBlock.getLoadedBlock();
        if (loadedValueBlock == valueBlock) {
            return this;
        }
        return createMapBlockInternal(
                startOffset,
                positionCount,
                Optional.ofNullable(mapIsNull),
                offsets,
                keyBlock,
                loadedValueBlock,
                hashTables,
                keyType,
                keyBlockNativeEquals,
                keyNativeHashCode,
                keyBlockHashCode);
    }

    @Override
    protected void ensureHashTableLoaded()
    {
        if (this.hashTables.get().isPresent()) {
            return;
        }

        // We need to synchronize access to the hashTables field as it may be shared by multiple MapBlock instances.
        synchronized (hashTables) {
            if (this.hashTables.get().isPresent()) {
                return;
            }

            int[] hashTables = new int[getRawKeyBlock().getPositionCount() * HASH_MULTIPLIER];
            Arrays.fill(hashTables, -1);
            for (int i = startOffset; i < startOffset + positionCount; i++) {
                int keyOffset = offsets[i];
                int keyCount = offsets[i + 1] - keyOffset;
                if (keyCount < 0) {
                    throw new IllegalArgumentException(format("Offset is not monotonically ascending. offsets[%s]=%s, offsets[%s]=%s", i, offsets[i], i + 1, offsets[i + 1]));
                }
                if (mapIsNull != null && mapIsNull[i] && keyCount != 0) {
                    throw new IllegalArgumentException("A null map must have zero entries");
                }
                buildHashTable(
                        getRawKeyBlock(),
                        keyOffset,
                        keyCount,
                        keyBlockHashCode,
                        hashTables,
                        keyOffset * HASH_MULTIPLIER,
                        keyCount * HASH_MULTIPLIER);
            }
            this.hashTables.set(hashTables);
        }
    }
}
