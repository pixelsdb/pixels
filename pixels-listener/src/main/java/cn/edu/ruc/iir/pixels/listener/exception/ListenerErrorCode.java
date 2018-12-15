package cn.edu.ruc.iir.pixels.listener.exception;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.*;

public enum ListenerErrorCode
        implements ErrorCodeSupplier
{
    PIXELS_EVENT_LISTENER_ERROR(1, EXTERNAL)
    /**/;

    private final ErrorCode errorCode;

    ListenerErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0100_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
