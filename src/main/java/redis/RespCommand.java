package redis;

import java.util.List;

public class RespCommand {

    private String command;
    private List<String> args;

    public RespCommand(String command, List<String> args) {
        this.command = command;
        this.args = args;
    }

    public String getCommand() {
        return command;
    }

    public List<String> getArgs() {
        return args;
    }

}
