package io.pixelsdb.pixels.daemon.scaling.util;

import java.util.Map;

public interface VmManager
{
    static final String PREFIX = "Auto-VM-";
    public String createInstance(String name);
    public void startInstance(String instanceId);
    public void stopInstance(String instanceId);
    public Map<String, ScalingManager.InstanceState> initInstanceStateMap();
}
