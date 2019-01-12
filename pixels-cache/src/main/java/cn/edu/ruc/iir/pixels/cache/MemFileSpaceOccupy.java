package cn.edu.ruc.iir.pixels.cache;

/**
 * pixels
 *
 * @author guodong
 */
public class MemFileSpaceOccupy
{
    public static void main(String[] args)
    {
        String name = args[0];
        long size = Long.parseLong(args[1]);

        String path = "/dev/shm/" + name;

        try {
            MemoryMappedFile file = new MemoryMappedFile(path, size);
            System.out.println("Created file with size: " + file.getSize());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
