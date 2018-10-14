package cn.edu.ruc.iir.pixels.cache;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 18-10-9
 * Author: hank
 */
public class PixelsCacheCoordinator extends Thread
{
    @Override
    public void run()
    {
        super.run();
    }

    public static void main(String[] args)
    {
        List<Integer>[] bucket = (List<Integer>[]) new List[1];
        bucket[0] = new ArrayList<Integer>();
        bucket[0].add(1);
        System.out.println(bucket[0].get(0).getClass().getTypeName());
    }
}
