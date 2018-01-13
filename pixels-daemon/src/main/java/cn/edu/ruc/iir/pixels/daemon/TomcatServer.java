package cn.edu.ruc.iir.pixels.daemon;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class TomcatServer implements Server
{

    public boolean isRunning()
    {
        //
        boolean serverIsOK = false;
        Process process = null;
        try
        {
            process = Runtime.getRuntime().exec("jps -l");

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null)
            {
                String[] strings = line.split("\\s{1,}");
                if (strings.length < 2)
                {
                    continue;
                }
                if (strings[1].contains("org.apache.catalina.startup.Bootstrap"))
                {
                    serverIsOK = true;
                    break;
                }
            }
            reader.close();
            process.destroy();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        return serverIsOK;
    }

    public void shutDownServer()
    {

    }

    @Override
    public void run()
    {
        System.out.println("Starting TomcatServer...");
        ProcessBuilder builder = new ProcessBuilder(new String[]{"/bin/bash", "-c", "/root/tomcat/bin/startup.sh"});
        try
        {
            builder.start();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
