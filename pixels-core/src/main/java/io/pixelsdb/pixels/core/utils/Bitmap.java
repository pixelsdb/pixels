/*
 * Copyright 2022 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.utils;

/**
 * Similar to java.util.BitSet. For performance considerations, Bitmap does not check the arguments
 * of its methods. Users of this class should be responsible for any errors caused by illegal arguments,
 * such as array-out-of-bound or negative bit-index.
 * <p/>
 * The implementation of some methods in this class referenced those in java.util.BitSet.
 * <p/>
 * Created at: 08/04/2022
 * Author: hank
 */
public class Bitmap
{
    /*
     * Bitmaps are packed into arrays of "words."  Currently a word is
     * a long, which consists of 64 bits, requiring 6 address bits.
     * The choice of word size is determined purely by performance concerns.
     */
    private final static int ADDRESS_BITS_PER_WORD = 6;
    private final static int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;
    private final static int BIT_INDEX_MASK = BITS_PER_WORD - 1;

    /* Used to shift left or right for a partial word mask */
    private static final long WORD_MASK = 0xffffffffffffffffL;

    /**
     * The internal field corresponding to the serialField "bits".
     */
    private final long[] words;
    private final int wordsInUse;
    private final int capacity;

    /**
     * Given a bit index, return word index containing it.
     */
    private static int wordIndex(int bitIndex)
    {
        return bitIndex >> ADDRESS_BITS_PER_WORD;
    }

    /**
     * Creates a bitmap whose initial size is large enough to explicitly
     * represent bits with indices in the range {@code 0} through
     * {@code capacity-1}. All bits are initialized to {@code value}.
     *
     * @param capacity the initial size of the bitmap
     * @param value    the initial value ot the bits
     * @throws NegativeArraySizeException if the specified initial size
     *                                    is negative
     */
    public Bitmap(int capacity, boolean value)
    {
        // nbits can't be negative; size 0 is OK
        if (capacity < 0)
            throw new NegativeArraySizeException("capacity < 0: " + capacity);
        words = new long[wordIndex(capacity - 1) + 1];
        wordsInUse = words.length;
        this.capacity = capacity;
        if (value)
        {
            setAll();
        }
    }

    private Bitmap(long[] words, int wordsInUse, int capacity)
    {
        this.words = words;
        this.wordsInUse = wordsInUse;
        this.capacity = capacity;
    }

    public void flip(int bitIndex)
    {
        int wordIndex = wordIndex(bitIndex);
        words[wordIndex] ^= (1L << bitIndex);
    }

    public void flip(int fromIndex, int toIndex)
    {
        if (fromIndex == toIndex)
            return;

        int startWordIndex = wordIndex(fromIndex);
        int endWordIndex = wordIndex(toIndex - 1);

        long firstWordMask = WORD_MASK << fromIndex;
        long lastWordMask = WORD_MASK >>> -toIndex;
        if (startWordIndex == endWordIndex)
        {
            // Case 1: One word
            words[startWordIndex] ^= (firstWordMask & lastWordMask);
        } else
        {
            // Case 2: Multiple words
            // Handle first word
            words[startWordIndex] ^= firstWordMask;

            // Handle intermediate words, if any
            for (int i = startWordIndex + 1; i < endWordIndex; i++)
                words[i] ^= WORD_MASK;

            // Handle last word
            words[endWordIndex] ^= lastWordMask;
        }
    }

    public void set(int bitIndex)
    {
        int wordIndex = wordIndex(bitIndex);
        words[wordIndex] |= (1L << bitIndex); // Restores invariants
    }

    public void set(int fromIndex, int toIndex)
    {
        if (fromIndex == toIndex)
            return;

        // Increase capacity if necessary
        int startWordIndex = wordIndex(fromIndex);
        int endWordIndex = wordIndex(toIndex - 1);

        long firstWordMask = WORD_MASK << fromIndex;
        long lastWordMask = WORD_MASK >>> -toIndex;
        if (startWordIndex == endWordIndex)
        {
            // Case 1: One word
            words[startWordIndex] |= (firstWordMask & lastWordMask);
        } else
        {
            // Case 2: Multiple words
            // Handle first word
            words[startWordIndex] |= firstWordMask;

            // Handle intermediate words, if any
            for (int i = startWordIndex + 1; i < endWordIndex; i++)
                words[i] = WORD_MASK;

            // Handle last word (restores invariants)
            words[endWordIndex] |= lastWordMask;
        }
    }

    public void setAll()
    {
        words[wordsInUse-1] |= (WORD_MASK >>> -this.capacity);
        int i = wordsInUse-1;
        while (i > 0)
            words[--i] = WORD_MASK;
    }

    public void clear(int bitIndex)
    {
        int wordIndex = wordIndex(bitIndex);
        words[wordIndex] &= ~(1L << bitIndex);
    }

    public void clear(int fromIndex, int toIndex)
    {
        if (fromIndex == toIndex)
            return;

        int startWordIndex = wordIndex(fromIndex);
        int endWordIndex = wordIndex(toIndex - 1);

        long firstWordMask = WORD_MASK << fromIndex;
        long lastWordMask = WORD_MASK >>> -toIndex;
        if (startWordIndex == endWordIndex)
        {
            // Case 1: One word
            words[startWordIndex] &= ~(firstWordMask & lastWordMask);
        } else
        {
            // Case 2: Multiple words
            // Handle first word
            words[startWordIndex] &= ~firstWordMask;

            // Handle intermediate words, if any
            for (int i = startWordIndex + 1; i < endWordIndex; i++)
                words[i] = 0;

            // Handle last word
            words[endWordIndex] &= ~lastWordMask;
        }
    }

    public void clearAll()
    {
        int i = wordsInUse;
        while (i > 0)
            words[--i] = 0;
    }

    public boolean get(int bitIndex)
    {
        int wordIndex = wordIndex(bitIndex);
        return ((words[wordIndex] & (1L << bitIndex)) != 0);
    }

    /**
     * Performs a logical <b>AND</b> of this target bitmap with the
     * argument bitmap. This bitmap is modified so that each bit in it
     * has the value {@code true} if and only if it both initially
     * had the value {@code true} and the corresponding bit in the
     * bitmap argument also had the value {@code true}.
     *
     * @param set a bitmap
     */
    public void and(Bitmap set)
    {
        if (this == set)
            return;

        int i = wordsInUse;
        while (i > set.wordsInUse)
            words[--i] = 0;

        // Perform logical AND on words in common
        for (i = 0; i < wordsInUse; i++)
            words[i] &= set.words[i];
    }

    /**
     * Performs a logical <b>OR</b> of this bitmap with the bitmap
     * argument. This bitmap is modified so that a bit in it has the
     * value {@code true} if and only if it either already had the
     * value {@code true} or the corresponding bit in the bitmap
     * argument has the value {@code true}.
     *
     * @param set a bitmap
     */
    public void or(Bitmap set)
    {
        if (this == set)
            return;

        int wordsInCommon = Math.min(wordsInUse, set.wordsInUse);

        // Perform logical OR on words in common
        for (int i = 0; i < wordsInCommon; i++)
            words[i] |= set.words[i];
    }

    /**
     * Performs a logical <b>XOR</b> of this bitmap with the bitmap
     * argument. This bitmap is modified so that a bit in it has the
     * value {@code true} if and only if one of the following
     * statements holds:
     * <ul>
     * <li>The bit initially has the value {@code true}, and the
     *     corresponding bit in the argument has the value {@code false}.
     * <li>The bit initially has the value {@code false}, and the
     *     corresponding bit in the argument has the value {@code true}.
     * </ul>
     *
     * @param set a bitmap
     */
    public void xor(Bitmap set)
    {
        int wordsInCommon = Math.min(wordsInUse, set.wordsInUse);

        // Perform logical XOR on words in common
        for (int i = 0; i < wordsInCommon; i++)
            words[i] ^= set.words[i];
    }

    /**
     * Clears all of the bits in this {@code Bitmap} whose corresponding
     * bit is set in the specified {@code Bitmap}.
     *
     * @param set the {@code Bitmap} with which to mask this
     *            {@code Bitmap}
     */
    public void andNot(Bitmap set)
    {
        // Perform logical (a & !b) on words in common
        for (int i = Math.min(wordsInUse, set.wordsInUse) - 1; i >= 0; i--)
            words[i] &= ~set.words[i];
    }

    /**
     * Returns true if the specified {@code Bitmap} has any bits set to
     * {@code true} that are also set to {@code true} in this {@code Bitmap}.
     *
     * @param set {@code Bitmap} to intersect with
     * @return boolean indicating whether this {@code Bitmap} intersects
     * the specified {@code Bitmap}
     */
    public boolean intersects(Bitmap set)
    {
        for (int i = Math.min(wordsInUse, set.wordsInUse) - 1; i >= 0; i--)
            if ((words[i] & set.words[i]) != 0)
                return true;
        return false;
    }

    /**
     * Returns the "logical size" of this {@code Bitmap}: the index of
     * the highest set bit in the {@code Bitmap} plus one. Returns zero
     * if the {@code Bitmap} contains no set bits.
     *
     * @return the logical size of this {@code Bitmap}
     */
    public int length()
    {
        if (wordsInUse == 0)
            return 0;

        int i = wordsInUse - 1;
        while (i >= 0)
        {
            if (words[i] == 0)
            {
                i--;
            } else
            {
                break;
            }
        }
        if (i < 0)
        {
            return 0;
        }

        return BITS_PER_WORD * i +
                (BITS_PER_WORD - Long.numberOfLeadingZeros(words[i]));
    }

    /**
     * @return the number of bits in this {@code Bitmap}.
     */
    public int capacity()
    {
        return capacity;
    }

    /**
     * Returns true if this {@code Bitmap} contains no bits that are set
     * to {@code true}.
     *
     * @return boolean indicating whether this {@code Bitmap} is empty
     */
    public boolean isEmpty()
    {
        int i = 0;
        while (i < wordsInUse)
        {
            if (words[i] != 0)
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if this {@code Bitmap} contains no bits that are set
     * to {@code false}.
     *
     * @return boolean indicating whether this {@code Bitmap} is full
     */
    public boolean isFull()
    {
        int i = 0;
        while (i < wordsInUse)
        {
            if (words[i] == 0)
            {
                return true;
            }
        }
        return true;
    }

    /**
     * @return the number of bits set to {@code true} in this {@code Bitmap}
     */
    public int cardinality()
    {
        int sum = 0;
        for (int i = 0; i < wordsInUse; i++)
            sum += Long.bitCount(words[i]);
        return sum;
    }

    /**
     * @param fromIndex from bit, inclusive
     * @param toIndex to bit, exclusive
     * @return the number of bits set to {@code true} from fromIndex (inclusive)
     * to toIndex (exclusive) in this {@code Bitmap}.
     */
    public int cardinality(int fromIndex, int toIndex)
    {
        if (fromIndex == toIndex)
            return 0;

        // Increase capacity if necessary
        int startWordIndex = wordIndex(fromIndex);
        int endWordIndex = wordIndex(toIndex - 1);

        long firstWordMask = WORD_MASK << fromIndex;
        long lastWordMask = WORD_MASK >>> -toIndex;
        if (startWordIndex == endWordIndex)
        {
            // Case 1: One word
            return Long.bitCount(
                    words[startWordIndex] & (firstWordMask & lastWordMask));
        } else
        {
            // Case 2: Multiple words
            // Handle first word
            int sum = Long.bitCount(
                    words[startWordIndex] & firstWordMask);

            // Handle intermediate words, if any
            for (int i = startWordIndex + 1; i < endWordIndex; i++)
                sum += Long.bitCount(words[i]);

            // Handle last word (restores invariants)
            sum += Long.bitCount(words[endWordIndex] & lastWordMask);
            return sum;
        }
    }

    /**
     * Returns the index of the first bit that is set to {@code true}
     * that occurs on or after the specified starting index. If no such
     * bit exists then {@code -1} is returned.
     *
     * <p>To iterate over the {@code true} bits in a {@code Bitmap},
     * use the following loop:
     *
     * <pre> {@code
     * for (int i = bm.nextSetBit(0); i >= 0; i = bb.nextSetBit(i+1)) {
     *     // operate on index i here
     *     if (i == Integer.MAX_VALUE) {
     *         break; // or (i+1) would overflow
     *     }
     * }}</pre>
     *
     * @param fromIndex the index to start checking from (inclusive)
     * @return the index of the next set bit, or {@code -1} if there
     * is no such bit
     */
    public int nextSetBit(int fromIndex)
    {
        int u = wordIndex(fromIndex);
        if (u >= wordsInUse)
            return -1;

        long word = words[u] & (WORD_MASK << fromIndex);

        while (true)
        {
            if (word != 0)
                return (u * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
            if (++u == wordsInUse)
                return -1;
            word = words[u];
        }
    }

    /**
     * Returns the index of the first bit that is set to {@code false}
     * that occurs on or after the specified starting index.
     *
     * @param fromIndex the index to start checking from (inclusive)
     * @return the index of the next clear bit
     */
    public int nextClearBit(int fromIndex)
    {

        int u = wordIndex(fromIndex);
        if (u >= wordsInUse)
            return fromIndex;

        long word = ~words[u] & (WORD_MASK << fromIndex);

        while (true)
        {
            if (word != 0)
                return (u * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
            if (++u == wordsInUse)
                return wordsInUse * BITS_PER_WORD;
            word = ~words[u];
        }
    }

    /**
     * Returns the index of the nearest bit that is set to {@code true}
     * that occurs on or before the specified starting index.
     * If no such bit exists, or if {@code -1} is given as the
     * starting index, then {@code -1} is returned.
     *
     * <p>To iterate over the {@code true} bits in a {@code Bitmap},
     * use the following loop:
     *
     * <pre> {@code
     * for (int i = bm.length(); (i = bm.previousSetBit(i-1)) >= 0; ) {
     *     // operate on index i here
     * }}</pre>
     *
     * @param fromIndex the index to start checking from (inclusive)
     * @return the index of the previous set bit, or {@code -1} if there
     * is no such bit
     * @throws IndexOutOfBoundsException if the specified index is less
     *                                   than {@code -1}
     */
    public int previousSetBit(int fromIndex)
    {
        if (fromIndex < 0)
        {
            if (fromIndex == -1)
                return -1;
            throw new IndexOutOfBoundsException(
                    "fromIndex < -1: " + fromIndex);
        }

        int u = wordIndex(fromIndex);
        if (u >= wordsInUse)
            return length() - 1;

        long word = words[u] & (WORD_MASK >>> -(fromIndex + 1));

        while (true)
        {
            if (word != 0)
                return (u + 1) * BITS_PER_WORD - 1 - Long.numberOfLeadingZeros(word);
            if (u-- == 0)
                return -1;
            word = words[u];
        }
    }

    /**
     * Returns the index of the nearest bit that is set to {@code false}
     * that occurs on or before the specified starting index.
     * If no such bit exists, or if {@code -1} is given as the
     * starting index, then {@code -1} is returned.
     *
     * @param fromIndex the index to start checking from (inclusive)
     * @return the index of the previous clear bit, or {@code -1} if there
     * is no such bit
     * @throws IndexOutOfBoundsException if the specified index is less
     *                                   than {@code -1}
     */
    public int previousClearBit(int fromIndex)
    {
        if (fromIndex < 0)
        {
            if (fromIndex == -1)
                return -1;
            throw new IndexOutOfBoundsException(
                    "fromIndex < -1: " + fromIndex);
        }

        int u = wordIndex(fromIndex);
        if (u >= wordsInUse)
            return fromIndex;

        long word = ~words[u] & (WORD_MASK >>> -(fromIndex + 1));

        while (true)
        {
            if (word != 0)
                return (u + 1) * BITS_PER_WORD - 1 - Long.numberOfLeadingZeros(word);
            if (u-- == 0)
                return -1;
            word = ~words[u];
        }
    }

    public Bitmap slice(int offset, int length) {
        if (offset < 0 || length < 0 || offset + length > length())
            throw new IndexOutOfBoundsException("offset: " + offset + ", length: " + length + ", length(): " + length());
        if (offset % BITS_PER_WORD != 0 || length % BITS_PER_WORD != 0)
            throw new IllegalArgumentException("offset and length must be multiple of " + BITS_PER_WORD);

        int wordOffset = wordIndex(offset);
        int wordLength = wordIndex(length - 1) + 1;
        long[] newWords = new long[wordLength];
        System.arraycopy(words, wordOffset, newWords, 0, wordLength);
        return new Bitmap(newWords, wordLength, length);
    }
}
