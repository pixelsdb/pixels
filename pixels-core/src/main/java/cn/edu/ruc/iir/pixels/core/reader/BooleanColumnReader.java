package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.TypeDescription;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * pixels
 *
 * @author guodong
 */
public class BooleanColumnReader
        extends ColumnReader
{
    public BooleanColumnReader(TypeDescription type)
    {
        super(type);
    }

    public boolean[] readBooleans(byte[] input, int num)
    {
        boolean[] values = new boolean[num];
        int index = 0;
        // input byte length should be GE than (num * 8) and be L than (num + 1) * 8
        checkArgument(input.length >= num * 8 && input.length < (num + 1) * 8,
                "Input bytes length is not correct");
        for (int i = 0; i < input.length && index < num; i++) {
            for (int j = 7; j >= 0; j--) {
                values[index] = ((0x1 << i) & input[i]) == (byte) 1;
                index++;
            }
        }
        return values;
    }
}
