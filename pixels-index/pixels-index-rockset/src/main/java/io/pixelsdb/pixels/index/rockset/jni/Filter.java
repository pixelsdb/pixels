package io.pixelsdb.pixels.index.rockset.jni;

public abstract class Filter extends RocksetHandle 
{
    protected Filter(long var1) 
    {
        super(var1);
    }

    protected void disposeInternal() 
    {
        this.disposeInternal(this.nativeHandle);
    }

    protected final void disposeInternal(long var1) 
    {
        disposeInternalJni(var1);
    }

    private static native void disposeInternalJni(long var0);
}
