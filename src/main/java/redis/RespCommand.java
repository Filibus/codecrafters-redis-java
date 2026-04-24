package redis;

import java.util.List;

public record RespCommand(String command, List<String> args) {
}
