package service;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class HttpService implements KVService {

    @NotNull
    private final HttpServer httpserver;
    @NotNull
    private final KVDao dao;
    @NotNull
    List<Map.Entry<String, Integer>> TopologyNodes;

    //Future: implement for storing DataID:NodeID information
    //private final static Map<String, String> MapNodeItemID = new HashMap<String, String>();

    protected static class QueryArgs {
        public String id;
        public int acks;
        public int replicas;

        QueryArgs(String id, int acks, int replicas){
            this.id = id; this.acks = acks; this.replicas = replicas;
        }
    }

    protected final static String regStrReplicas = "id=([a-z0-9]+)&replicas=(\\d)/(\\d)";//"id=(a-z0-9)&replicas=(\\d)/(\\d)";
    protected final static String regStrId = "id=([a-z0-9]+)";
    protected final static String idStr = "id=";
    protected final static String replicasStr = "replicas=";

    protected final static int NumberOfAcks  = 1;
    protected final static int NumberOfReplicas  = 1;

    @NotNull
    protected QueryArgs checkRequest(@NotNull final String query){
        if (query == null){
            throw new IllegalArgumentException("Invalid argument");
        }

        Pattern patternReplicas = Pattern.compile(regStrReplicas);
        Matcher matchReplicas = patternReplicas.matcher(query);

        Pattern patternId = Pattern.compile(regStrId);
        Matcher matchId = patternId.matcher(query);

        if(matchReplicas.matches()) {
            // f = matchReplicas.group(0);
            return new QueryArgs(matchReplicas.group(1),
                                Integer.parseInt(matchReplicas.group(2)),
                                Integer.parseInt(matchReplicas.group(3)));
        }

        if(matchId.matches()) {
            return new QueryArgs(matchId.group(1), NumberOfAcks ,NumberOfReplicas);
        }
        else{
            throw new IllegalArgumentException("Invalid argument");
        }
    }

    @NotNull
    private static String extractId(@NotNull final String query){

        if(!query.startsWith(idStr)){
            throw new IllegalArgumentException("Invalid argument");
        }

        final String id = query.substring(idStr.length());

        if (id.isEmpty()){
            throw new IllegalArgumentException();
        }

        return id;
    }

    private void storeTopologyNodes(final int address, @NotNull final int port, @NotNull final Set<String> topology) {
        List<Map.Entry<String, Integer>> TopologyMap = new ArrayList<>();
        Boolean AddressInTopology = false;

        for (String s : topology){
            int ixHostColon = s.indexOf("://");
            int ixPortColon = s.lastIndexOf(":");
            String addr = s.substring(ixHostColon+3, ixPortColon);
            Integer prt = Integer.parseInt(s.substring(ixPortColon+1));
            TopologyMap.add(Map.entry(addr, prt));

            if (address != 0 && address == Integer.parseInt(addr) && port == prt)
                AddressInTopology = true;
            if (address == 0 && port == prt) // Used in the test case when address = localhost
                AddressInTopology = true;
        }

        if(!AddressInTopology)
            throw new IllegalArgumentException("Port doesn't belong to provided topology");

        int CurrentNodeIdx = 0;
        int MapSize = TopologyMap.size();

        while(TopologyMap.get(CurrentNodeIdx).getValue() != port)
            CurrentNodeIdx++;

        for(int i = 0; i < MapSize; i++){
            TopologyNodes.add(TopologyMap.get((CurrentNodeIdx+i) % MapSize));
        }
    }

    private static URI uriForward(URI uri, String address, int port){
        try {
            Pattern patternID = Pattern.compile(regStrId);
            Matcher matcherID = patternID.matcher(uri.getQuery());

            String newQuery;

            if (!matcherID.find()){
                throw new IllegalArgumentException("Invalid argument");
            }
            newQuery = matcherID.group(0).concat("&replicas=1/1");
            URI NewURI = new URI("http", uri.getUserInfo(), address, port,
                    uri.getPath(), newQuery, uri.getFragment());

            return NewURI;
        } catch (URISyntaxException e){
            return null;
        }
    }

    public HttpService(@NotNull final int port, @NotNull final KVDao dao,
                       @NotNull final Set<String> topology) throws IOException {

        this.httpserver = HttpServer.create(
                new InetSocketAddress(port),
                0);

        this.dao = dao;
        this.TopologyNodes = new ArrayList<>();

        storeTopologyNodes(0, port, topology);

        this.httpserver.createContext(
                "/",
                http_hl -> {
                    final String response = "No content for this request..";
                    http_hl.sendResponseHeaders(400, response.length());
                    http_hl.getResponseBody().write(response.getBytes());

                    //TODO: check this
                    http_hl.close();
                }
        );

        this.httpserver.createContext(
                "/v0/status",
                http_hl -> {
                        final String response = "Server Online..";
                        http_hl.sendResponseHeaders(200, response.length());
                        http_hl.getResponseBody().write(response.getBytes());

                        //TODO: check this
                        http_hl.close();
                    }
                );

        this.httpserver.createContext(
                "/v0/entity", //?id=
                new ErrorHandler(http_hl -> {
                        QueryArgs QueryArguments = checkRequest(http_hl.getRequestURI().getQuery());

                        //Forward the request to the required port
                        URI InputURI = http_hl.getRequestURI();

                        if (QueryArguments.replicas > topology.size())
                            throw new IllegalArgumentException("Exc: Topology size is lower than requested FROM nodes");

                        if (QueryArguments.acks > QueryArguments.replicas)
                            throw new IllegalArgumentException("Exc: Amount of requested ACK nodes is lower than FROM nodes");

                        if (QueryArguments.acks == 0)
                            throw new IllegalArgumentException("Exc: Requested amount ACK nodes is 0");

                        //create here new URI

                        //Single replicas get complete flag
                        boolean doProceedToNodes = true;
                        boolean isCurrentNodeOk = false;

                        //Count Replicas
                        AtomicInteger ReplicasAckCnt = new AtomicInteger(0);
                        AtomicInteger ReplicasNAckCnt = new AtomicInteger(0);

                        switch(http_hl.getRequestMethod()){
                            case "GET":
                                //Contains value from of the current Node
                                final byte[] getValue;

                                //ObjectHist is used to implement max object counting
                                Map<String, Integer> ObjectHist = new HashMap<>();

                                try {
                                    getValue = dao.get(QueryArguments.id.getBytes());

                                    if (QueryArguments.replicas == 1) {
                                        http_hl.sendResponseHeaders(200, getValue.length);
                                        http_hl.getResponseBody().write(getValue);
                                        doProceedToNodes = false;
                                        break;
                                    }
                                    isCurrentNodeOk = true;
                                    ReplicasAckCnt.getAndIncrement();
                                    //Put the value from current
                                    ObjectHist.put(new String(getValue), 1);
                                }catch (NoSuchElementException e){
                                    if (QueryArguments.replicas == 1) {
                                        doProceedToNodes = false;
                                        http_hl.sendResponseHeaders(404, 0);
                                        http_hl.close();
                                    }
                                } catch (IllegalArgumentException e){
                                    if (QueryArguments.replicas == 1) {
                                        doProceedToNodes = false;
                                        http_hl.sendResponseHeaders(400, 0);
                                        http_hl.close();
                                    }
                                }

                                if(!doProceedToNodes)
                                    break;

                                for (int i = 1; i < QueryArguments.replicas; i++){
                                    //Get ports: always next n replica nodes from the original topology set
                                    URI NextURI = uriForward(InputURI,
                                            this.TopologyNodes.get(i).getKey(),
                                            this.TopologyNodes.get(i).getValue());

                                    if (NextURI == null){
                                        continue;
                                    }

                                    HttpRequest request = HttpRequest.newBuilder().
                                                    uri(NextURI).
                                                    GET().
                                                    build();

                                    HttpClient client = HttpClient.newHttpClient();

                                    client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).
                                            orTimeout(1, TimeUnit.SECONDS).
                                            whenComplete((response, error) -> {
                                                        if(error == null){
                                                            if (response.statusCode() == 200) {
                                                                ReplicasAckCnt.getAndIncrement();

                                                                String ResponseBody = response.body();

                                                                if (!ObjectHist.containsKey(ResponseBody))
                                                                    ObjectHist.put(ResponseBody, 1);
                                                                else
                                                                    ObjectHist.put(ResponseBody, ObjectHist.get(ResponseBody) + 1);
                                                            }else
                                                                ReplicasNAckCnt.getAndIncrement();
                                                        }
                                                        else
                                                            ReplicasNAckCnt.getAndIncrement();
                                                    }
                                            );
                                }

                                /*Now wait until all pending requests are over*/
                                if (isCurrentNodeOk) {
                                    while (ReplicasAckCnt.get() + ReplicasNAckCnt.get()
                                            < QueryArguments.replicas
                                            &&
                                            ReplicasAckCnt.get() < QueryArguments.acks);
                                }else{
                                    while (ReplicasAckCnt.get() + ReplicasNAckCnt.get()
                                            < (QueryArguments.replicas - 1)
                                            &&
                                            ReplicasAckCnt.get() < QueryArguments.acks);
                                }

                                // Plausibility check for the equality of the collected data
                                int MaxSameReplies = 0;
                                String MaxValueString = "";
                                for (String k : ObjectHist.keySet()){
                                    int InsertionCnt = ObjectHist.get(k);
                                    if (InsertionCnt > MaxSameReplies){
                                        MaxSameReplies = InsertionCnt;
                                        MaxValueString = k;
                                    }
                                }

                                ReplicasAckCnt.set(MaxSameReplies);

                                if (ReplicasAckCnt.get() >= QueryArguments.acks){
                                    http_hl.sendResponseHeaders(200, MaxValueString.length());
                                    http_hl.getResponseBody().write(MaxValueString.getBytes());
                                } else{
                                    //Not Enough Replicas
                                    http_hl.sendResponseHeaders(504, 0);
                                }

                                break;

                            case "PUT":
                                int readLength = 0;
                                final int contentLength =
                                        Integer.valueOf(http_hl.getRequestHeaders().getFirst("content-length"));

                                byte[] putValue = new byte[contentLength];

                                if (contentLength != 0) {
                                    InputStream is = http_hl.getRequestBody();

                                    try {
                                        readLength = is.read(putValue);
                                    }catch (IOException e){
                                        if (QueryArguments.replicas == 1) {
                                            doProceedToNodes = false;
                                            http_hl.sendResponseHeaders(404, 0);
                                            http_hl.close();
                                        }
                                    }

                                    if (readLength != contentLength && QueryArguments.replicas == 1) {
                                            doProceedToNodes = false;
                                            http_hl.sendResponseHeaders(404, 0);
                                            http_hl.close();
                                    }

                                }else{
                                    putValue = "".getBytes();
                                }

                                if(!doProceedToNodes)
                                    break;

                                try{
                                    dao.upsert(QueryArguments.id.getBytes(), putValue);
                                    // If request requires only one replica, then immediately upsert and stop
                                    if (QueryArguments.replicas == 1) {
                                        http_hl.sendResponseHeaders(201, 0);
                                        doProceedToNodes = false;
                                        break;
                                    }
                                    isCurrentNodeOk = true;
                                    ReplicasAckCnt.getAndIncrement();
                                }catch (NoSuchElementException e){
                                    if (QueryArguments.replicas == 1) {
                                        doProceedToNodes = false;
                                        http_hl.sendResponseHeaders(404, 0);
                                        http_hl.close();
                                    }
                                } catch (IllegalArgumentException e){
                                    if (QueryArguments.replicas == 1) {
                                        doProceedToNodes = false;
                                        http_hl.sendResponseHeaders(400, 0);
                                        http_hl.close();
                                    }
                                }

                                if(!doProceedToNodes)
                                    break;

                                for (int i = 1; i < QueryArguments.replicas; i++) {
                                    //Get ports: always next n replica nodes from the original topology set
                                    URI NextURI = uriForward(InputURI,
                                            this.TopologyNodes.get(i).getKey(),
                                            this.TopologyNodes.get(i).getValue());

                                    if (NextURI == null) {
                                        continue;
                                    }

                                    //Future: store ID:NodePort maps on a separate Node.
                                    //Duration d = Duration.ofSeconds(1);
                                    HttpRequest request = HttpRequest.newBuilder().
                                            uri(NextURI).
                                            /*timeout(d).*/
                                            PUT(HttpRequest.BodyPublishers.ofByteArray(putValue)).
                                            build();

                                    HttpClient client = HttpClient.newHttpClient();

                                    client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).
                                            thenApply(HttpResponse::statusCode).
                                            orTimeout( 1, TimeUnit.SECONDS).
                                            whenComplete((statusCode, error) -> {
                                                if (error == null) {
                                                    if (statusCode == 201)
                                                        ReplicasAckCnt.getAndIncrement();
                                                    else
                                                        ReplicasNAckCnt.getAndIncrement();
                                                } else
                                                    ReplicasNAckCnt.getAndIncrement();
                                            });
                                }

                                /*Now wait until all pending requests are over*/
                                if (isCurrentNodeOk) {
                                    while (ReplicasAckCnt.get() + ReplicasNAckCnt.get()
                                            < QueryArguments.replicas
                                            &&
                                            ReplicasAckCnt.get() < QueryArguments.acks);
                                }else{
                                    while (ReplicasAckCnt.get() + ReplicasNAckCnt.get()
                                            < (QueryArguments.replicas - 1)
                                            &&
                                            ReplicasAckCnt.get() < QueryArguments.acks);
                                }

                                if (ReplicasAckCnt.get() >= QueryArguments.acks){
                                    http_hl.sendResponseHeaders(201, 0);
                                } else{
                                    //Not Enough Replicas
                                    http_hl.sendResponseHeaders(504, 0);
                                }

                                break;

                            case "DELETE":
                                try{
                                    dao.remove(QueryArguments.id.getBytes());
                                    // If request requires only one replica, then immediately upsert and stop
                                    if (QueryArguments.replicas == 1) {
                                        http_hl.sendResponseHeaders(202, 0);
                                        doProceedToNodes = false;
                                        break;
                                    }
                                    isCurrentNodeOk = true;
                                    ReplicasAckCnt.getAndIncrement();
                                }catch (NoSuchElementException e){
                                    if (QueryArguments.replicas == 1) {
                                        doProceedToNodes = false;
                                        http_hl.sendResponseHeaders(404, 0);
                                        http_hl.close();
                                    }
                                } catch (IllegalArgumentException e){
                                    if (QueryArguments.replicas == 1) {
                                        doProceedToNodes = false;
                                        http_hl.sendResponseHeaders(400, 0);
                                        http_hl.close();
                                    }
                                }

                                if (!doProceedToNodes)
                                    break;

                                for (int i = 1; i < QueryArguments.replicas; i++) {
                                    //Get ports: always next n replica nodes from the original topology set
                                    URI NextURI = uriForward(InputURI,
                                            this.TopologyNodes.get(i).getKey(),
                                            this.TopologyNodes.get(i).getValue());

                                    if (NextURI == null) {
                                        continue;
                                    }

                                    //Future: store ID:NodePort maps on a separate Node.
                                    Duration d = Duration.ofMillis(1000);
                                    HttpRequest request = HttpRequest.newBuilder().
                                            uri(NextURI).
                                            timeout(d).
                                            DELETE().
                                            build();

                                    HttpClient client = HttpClient.newHttpClient();

                                    client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).
                                            thenApply(HttpResponse::statusCode).
                                            orTimeout( 1, TimeUnit.SECONDS).
                                            whenComplete((statusCode, error) -> {
                                                if (error == null) {
                                                    if (statusCode == 202)
                                                        ReplicasAckCnt.getAndIncrement();
                                                    else
                                                        ReplicasNAckCnt.getAndIncrement();
                                                } else
                                                    ReplicasNAckCnt.getAndIncrement();
                                            });
                                }

                                /*Now wait until all pending requests are over*/
                                if (isCurrentNodeOk) {
                                    while (ReplicasAckCnt.get() + ReplicasNAckCnt.get()
                                            < QueryArguments.replicas
                                            &&
                                            ReplicasAckCnt.get() < QueryArguments.acks);
                                }else{
                                    while (ReplicasAckCnt.get() + ReplicasNAckCnt.get()
                                            < (QueryArguments.replicas - 1)
                                            &&
                                            ReplicasAckCnt.get() < QueryArguments.acks);
                                }

                                if (ReplicasAckCnt.get() >= QueryArguments.acks){
                                    http_hl.sendResponseHeaders(202, 0);
                                } else{
                                    //Not enough replicas
                                    http_hl.sendResponseHeaders(504, 0);
                                }
                                break;
                            default:
                                http_hl.sendResponseHeaders(405, 0);
                                break;
                        }

                        http_hl.close();
                    }
                )
        );
    }

    @Override
    public void start() {
        this.httpserver.start();
    }

    @Override
    public void stop() {
        this.httpserver.stop(0);
    }

    private static class ErrorHandler implements HttpHandler{
        private final HttpHandler delegate;

        private ErrorHandler(HttpHandler delegate){
            this.delegate = delegate;
        }

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            try {
                delegate.handle(httpExchange);
            } catch (NoSuchElementException e){
                httpExchange.sendResponseHeaders(404, 0);
                httpExchange.close();
            } catch (IllegalArgumentException e){
                System.out.println(e.toString());
                httpExchange.sendResponseHeaders(400, 0);
                httpExchange.close();
            }
        }
    }
}
