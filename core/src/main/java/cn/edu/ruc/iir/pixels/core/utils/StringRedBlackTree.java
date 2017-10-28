package cn.edu.ruc.iir.pixels.core.utils;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A red-black tree that stores strings. The strings are stored as UTF-8 bytes
 * and an offset for each entry.
 */
public class StringRedBlackTree extends RedBlackTree
{
    private final DynamicByteArray byteArray = new DynamicByteArray();
    private final DynamicIntArray keyOffsets;
    private String newKey;

    public StringRedBlackTree(int initialCapacity)
    {
        super(initialCapacity);
        keyOffsets = new DynamicIntArray(initialCapacity);
    }

    public int add(String value)
    {
        newKey = value;
        // if the newKey is actually new, add it to our byteArray and store the offset & length
        if (add())
        {
            int len = newKey.length();
            keyOffsets.add(byteArray.add(newKey.getBytes(), 0, len));
        }
        return lastAdd;
    }

    // compare newKey with position specified key
    @Override
    protected int compareValue(int position)
    {
        int start = keyOffsets.get(position);
        int end;
        if (position + 1 == keyOffsets.size())
        {
            end = byteArray.size();
        } else
        {
            end = keyOffsets.get(position + 1);
        }
        return byteArray.compare(newKey.getBytes(), 0, newKey.length(),
                start, end - start);
    }

    /**
     * The information about each node.
     */
    public interface VisitorContext
    {
        /**
         * Get the position where the key was originally added.
         *
         * @return the number returned by add.
         */
        int getOriginalPosition();

        /**
         * Write the bytes for the string to the given output stream.
         *
         * @param out the stream to write to.
         * @throws IOException
         */
        void writeBytes(OutputStream out) throws IOException;

        /**
         * Get the original string.
         *
         * @return the string
         */
        String getString();

        /**
         * Get the number of bytes.
         *
         * @return the string's length in bytes
         */
        int getLength();
    }

    /**
     * The interface for visitors.
     */
    public interface Visitor
    {
        /**
         * Called once for each node of the tree in sort order.
         *
         * @param context the information about each node
         * @throws IOException
         */
        void visit(VisitorContext context) throws IOException;
    }

    private class VisitorContextImpl implements VisitorContext
    {
        private int originalPosition;
        private int start;
        private int end;
        private final Text text = new Text();

        public int getOriginalPosition()
        {
            return originalPosition;
        }

        public String getString()
        {
            byteArray.setText(text, start, end - start);
            return text.toString();
        }

        public void writeBytes(OutputStream out) throws IOException
        {
            byteArray.write(out, start, end - start);
        }

        public int getLength()
        {
            return end - start;
        }

        void setPosition(int position)
        {
            originalPosition = position;
            start = keyOffsets.get(originalPosition);
            if (position + 1 == keyOffsets.size())
            {
                end = byteArray.size();
            } else
            {
                end = keyOffsets.get(originalPosition + 1);
            }
        }
    }

    private void recurse(int node, Visitor visitor, VisitorContextImpl context
    ) throws IOException
    {
        if (node != NULL)
        {
            recurse(getLeft(node), visitor, context);
            context.setPosition(node);
            visitor.visit(context);
            recurse(getRight(node), visitor, context);
        }
    }

    /**
     * Visit all of the nodes in the tree in sorted order.
     *
     * @param visitor the action to be applied to each node
     * @throws IOException
     */
    public void visit(Visitor visitor) throws IOException
    {
        recurse(root, visitor, new VisitorContextImpl());
    }

    /**
     * Reset the table to empty.
     */
    public void clear()
    {
        super.clear();
        byteArray.clear();
        keyOffsets.clear();
    }

    public String getString(int originalPosition)
    {
        Text result = new Text();
        int offset = keyOffsets.get(originalPosition);
        int length;
        if (originalPosition + 1 == keyOffsets.size())
        {
            length = byteArray.size() - offset;
        } else
        {
            length = keyOffsets.get(originalPosition + 1) - offset;
        }
        byteArray.setText(result, offset, length);
        return result.toString();
    }

    /**
     * Get the size of the character data in the table.
     *
     * @return the bytes used by the table
     */
    public int getCharacterSize()
    {
        return byteArray.size();
    }
}
