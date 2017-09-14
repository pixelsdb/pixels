package cn.edu.ruc.iir.rainbow.common.cmd;

import cn.edu.ruc.iir.rainbow.common.exception.CommandException;
import cn.edu.ruc.iir.rainbow.common.exception.InvokerException;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by hank on 17-5-4.
 */
abstract public class Invoker
{
    private List<Command> commands = new ArrayList<>();

    protected Invoker ()
    {
        this.createCommands();
    }

    /**
     * create this.command and set receiver for it
     */
    abstract protected void createCommands ();

    final protected void addCommand(Command command) throws CommandException
    {
        if (command != null)
        {
            this.commands.add(command);
        }
        else
        {
            throw new CommandException("command is null.");
        }
    }

    public final void executeCommands (Properties params) throws InvokerException
    {
        for (Command command : this.commands)
        {
            command.execute(params);
        }
    }
}
