package service;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.IOException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//UNUSED

public class HttpServiceSatellite extends HttpService {

    protected final static int NumberOfAcks  = 1;
    protected final static int NumberOfReplicas  = 1;

    public HttpServiceSatellite(@NotNull final int port, @NotNull final KVDao dao,
                                 @NotNull final Set<String> topology) throws IOException {
        super(port, dao, topology);
    }

    @Override
    @NotNull
    protected QueryArgs checkRequest(@NotNull final String query){
        Pattern patternReplicas = Pattern.compile(regStrReplicas);
        Matcher matchReplicas = patternReplicas.matcher(query);

        Pattern patternId = Pattern.compile(regStrId);
        Matcher matchId = patternId.matcher(query);

        if(matchReplicas.matches()) {
            return new QueryArgs(matchReplicas.group(0),
                    Integer.parseInt(matchReplicas.group(1)) ,
                    Integer.parseInt(matchReplicas.group(2)));
        }

        if(matchId.matches()) {
            return new QueryArgs(matchId.group(0), NumberOfAcks ,NumberOfReplicas);
        }
        else{
            throw new IllegalArgumentException("Invalid argument");
        }
    }
}
