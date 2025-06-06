
// to run from clojure (test_with_uuid_a/main (into-array [str1 str2 str3 str4]))
// Accepts command-line arguments for hosts, port, uuid and ipAddress.
// Usage:
/////   > java -jar test_with_uuid_a.jar   -h
/////   Usage: java -jar test_with_uuid_a.jar <host1,...> <port> <uuid> <ipAddress>
/////     e.g. java -jar test_with_uuid_a.jar  nytestsapi01  9294  7420832  10.137.42.184
//
// <hosts>      : comma-separated list of hostnames
// <port>       : integer port (same for every host)
// <uuid>       : integer EMRS UUID allocated to the BLPAPI application
// <ipAddress>  : string IPv4 address registered with EMRS
//
// NOTE: Only the minimal changes required to support runtime parameters are
// included; business logic remains identical to the original sample.
// -----------------------------------------------------------------------------

// Added a config class and file input for instruments and fields.

/*
 * Copyright 2023 Bloomberg Finance L.P.
 *
 * Sample code provided by Bloomberg is made available for illustration purposes
 * only. Sample code is modifiable by individual users and is not reviewed for
 * reliability, accuracy and is not supported as part of any Bloomberg service.
 * Users are solely responsible for the selection of and use or intended use of
 * the sample code, its applicability, accuracy and adequacy, and the resultant
 * output thereof. Sample code is proprietary and confidential to Bloomberg and
 * neither the recipient nor any of its representatives may distribute, publish
 * or display such code to any other party, other than information disclosed to
 * its employees on a need-to-know basis in connection with the purpose for which
 * such code was provided. Sample code provided by Bloomberg is provided without
 * any representations or warranties and subject to modification by Bloomberg in
 * its sole discretion.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL BLOOMBERG BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
 * USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

import com.bloomberglp.blpapi.CorrelationID;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.Event.EventType;
import com.bloomberglp.blpapi.EventHandler;
import com.bloomberglp.blpapi.Logging;

import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.Datetime;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.EventQueue;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Subscription;
import com.bloomberglp.blpapi.SubscriptionList;
import com.bloomberglp.blpapi.Identity;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

//private
class Config
{
    enum   ConnectionType{TLS,TLS_LL,NO_TLS};
    public static final ConnectionType connectionType = ConnectionType.NO_TLS;

    public static String[] hosts      = {
            "nytestsapi01"
    };

    public static int    port         = 9294;
    public static int    uuid         = 7420832;
    public static String ipAddress    = "10.137.42.184";
    public static final int maxsec    = 400;

    public static ArrayList<String> topics =
            new ArrayList<String>(Arrays.asList(
                    "USDXAU Curncy"      ,
                    "USDXAU BGN Curncy"  ,
                    "USDXAU CMPN Curncy" ,
                    "BBHBEAT Index"
            ));

    public static ArrayList<String> fields =
            new ArrayList<String>(Arrays.asList(
                    "MKTDATA_EVENT_TYPE"         ,
                    "MKTDATA_EVENT_SUBTYPE"      ,
                    "IS_DELAYED_STREAM"          ,
                    "RT_PRICING_SOURCE"          ,
                    "FEED_SOURCE_BPIPE_REALTIME" ,
                    "EID"                        ,
                    "BID"                        ,
                    "ASK"                        ,
                    "LAST_PRICE"
            ));

    public static String[] sub_options = {
            // "interval=10"
    };
}


public class test_with_uuid_a
{
    private static final Name SLOW_CONSUMER_WARNING         = Name.getName( "SlowConsumerWarning"        );
    private static final Name SLOW_CONSUMER_WARNING_CLEARED = Name.getName( "SlowConsumerWarningCleared" );
    private static final Name DATA_LOSS                     = Name.getName( "DataLoss"                   );
    private static final Name SUBSCRIPTION_TERMINATED       = Name.getName( "SubscriptionTerminated"     );
    private static final Name SOURCE                        = Name.getName( "source"                     );
    private static final Name AUTHORIZATION_SUCCESS         = Name.getName( "AuthorizationSuccess"       );
    private static final Name AUTHORIZATION_FAILURE         = Name.getName( "AuthorizationFailure"       );
    private static final Name AUTHORIZATION_REVOKED         = Name.getName( "AuthorizationRevoked"       );
    private static final Name TOKEN_SUCCESS                 = Name.getName( "TokenGenerationSuccess"     );
    private static final Name TOKEN                         = Name.getName( "token"                      );

    private Session                  d_session;
    private ArrayList<String>        d_topics;
    private SubscriptionList         d_subscriptions;
    private SimpleDateFormat         d_dateFormat;
    private String                   d_service;
    private boolean                  d_isSlow;
    private boolean                  d_isStopped;
    private final SubscriptionList   d_pendingSubscriptions;
    private final Set<CorrelationID> d_pendingUnsubscribe;
    private final Object             d_lock;
    private Identity                 d_identity;
    private CorrelationID            d_authCorrelationId;

    enum                             AuthStatus{FAILED_AUTHORIZATION, NOT_AUTHORIZED, AUTHORIZED};
    private AuthStatus               d_authStatus = AuthStatus.NOT_AUTHORIZED;

    /**
     * @param args
     */
    public static void main(String[] args) throws java.lang.Exception
    {
        System.out.println("Realtime Event Handler Example");
        test_with_uuid_a example = new test_with_uuid_a();
        example.run(args);
    }

    public test_with_uuid_a()
    {
        d_service              = "//blp/mktdata";
        d_topics               = new ArrayList<String>();
        d_subscriptions        = new SubscriptionList();
        d_dateFormat           = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        d_isSlow               = false;
        d_isStopped            = false;
        d_pendingSubscriptions = new SubscriptionList();
        d_pendingUnsubscribe   = new HashSet<CorrelationID>();
        d_lock                 = new Object();

    }

    private boolean createSession() throws Exception
    {
        if (d_session != null) d_session.stop();
        SessionOptions  options = new SessionOptions();

        // Server addresses setup
        SessionOptions.ServerAddress[] servers = new SessionOptions.ServerAddress[1];
        servers[0] = new SessionOptions.ServerAddress(Config.hosts[0], Config.port);
        options.setServerAddresses(servers);


        System.out.println("Session options: " + options.toString());
        d_session = new Session(options, new SubscriptionEventHandler(d_topics, d_subscriptions));
        System.out.println("Starting session...\n");
        if (!d_session.start()) {
            System.err.println("Failed to start session\n");
            return false;
        }
        System.out.println("Connected successfully\n");

        if (!d_session.openService(d_service)) {
            System.err.printf("Failed to open service: %s%n", d_service);
            d_session.stop();
            return false;
        }

        return true;
    }

    private boolean authorize( Service authService, Session session, Identity identity, CorrelationID cid)
            throws IOException, InterruptedException
    {
        boolean is_authorized = false;
        final int WAIT_TIME_SECONDS = 60;

        Request authRequest = authService.createAuthorizationRequest();
        authRequest.set("uuid", Config.uuid);
        authRequest.set("ipAddress", Config.ipAddress);


        EventQueue authEventQueue = new EventQueue();
        d_session.sendAuthorizationRequest(authRequest, identity, authEventQueue, cid);

        while (true)
        {
            Event event = authEventQueue.nextEvent(WAIT_TIME_SECONDS * 1000);
            if (event.eventType() == EventType.RESPONSE
                    || event.eventType() == EventType.PARTIAL_RESPONSE)
            {
                if (event.eventType() == EventType.PARTIAL_RESPONSE)
                {
                    System.out.println("Warning: Received authorization partial response. The authorization requestion should be sent asynchronously.");
                }
                for (Message msg: event)
                {
                    if (msg.messageType() == AUTHORIZATION_SUCCESS)
                    {
                        System.out.println("User " + Config.uuid + " authorization success");
                        is_authorized = true;
                    }
                    else
                    {
                        System.out.println("User " + Config.uuid + " authorization failed");
                        System.out.println(msg.toString());
                    }
                }
            }
            else if (event.eventType() == EventType.REQUEST_STATUS)
            {
                // request failure
                for (Message msg: event)
                {
                    System.out.println("REQUEST_STATUS: " + msg.toString());
                }
            }
            else if (event.eventType() == EventType.TIMEOUT)
            {
                System.out.println("Authorization response did not return within the timeout given timeout period");
                // cancel generate token request
                session.cancel(cid);
            }
            break;
        }
        return is_authorized;
    }

    private void subscribe() throws Exception {
        int cid = 0;
        for(String line: Config.topics) {
            d_topics.add(line);
            d_subscriptions.add(new Subscription(line,
                    Config.fields, Arrays.asList(Config.sub_options), new CorrelationID(++cid)));
            if(cid==Config.maxsec){
                break;
            }
        }

        System.out.print("Use simplified auth credential");
        d_session.subscribe(d_subscriptions, d_identity);
    }


    private void run(String[] args) throws Exception
    {
        // turn off logging by default
        registerCallback(Level.OFF);

        if (args.length != 4) {
            System.err.println("Usage: java -jar test_with_uuid_a.jar <host1,...> <port> <uuid> <ipAddress>");
            System.err.printf("  e.g. java -jar test_with_uuid_a.jar  %s  %d  %d  %s\n", Config.hosts[0], Config.port, Config.uuid, Config.ipAddress);
            System.exit(1);
        }

        // Populate mutable fields in Config from command line
        Config.hosts     = args[0].split(",");
        Config.port      = Integer.parseInt(args[1]);
        Config.uuid      = Integer.parseInt(args[2]);
        Config.ipAddress = args[3];

        System.out.println("\nOptions to be used:");
        System.out.printf("| Config.hosts   : %s\n", args[0]);
        System.out.printf("| Config.port    : %s\n", args[1]);
        System.out.printf("| Config.uuid    : %s\n", args[2]);
        System.out.printf("| Config.ipAdress: %s\n", args[3]);
        System.out.println();

        if (!createSession()) return;

        boolean isAuthorized = false;
        d_identity = d_session.createIdentity();
        d_authCorrelationId = new CorrelationID("authCorrelation");
        if (d_session.openService("//blp/apiauth"))
        {
            Service authService = d_session.getService("//blp/apiauth");
            isAuthorized = authorize(authService, d_session, d_identity, d_authCorrelationId);
        }
        else
        {
            System.err.println("Failed to open //blp/apiauth.");
        }
        if (!isAuthorized)
        {
            System.err.println("No authorization");
            return;
        }

        System.out.println("Subscribing...");
        subscribe();

        // wait for enter key to exit application
        System.in.read();
        synchronized (d_lock) {
            d_isStopped = true;
        }

        d_session.cancel(d_authCorrelationId);
        d_session.stop();
        System.out.println("Exiting...");
    }

    // register API logging callback level
    private void registerCallback(Level logLevel)
    {
        Logging.Callback loggingCallback = new Logging.Callback() {
            @Override
            public void onMessage(long threadId, Level level, Datetime dateTime,
                                  String loggerName, String message) {
                System.out.println(dateTime + "  " + loggerName + " [" + level.toString() + "] Thread ID = "
                        + threadId + " " + message);
            }
        };

        Logging.registerCallback(loggingCallback, logLevel);
    }

    class SubscriptionEventHandler implements EventHandler
    {
        ArrayList<String> d_topics;
        SubscriptionList d_subscriptions;

        public SubscriptionEventHandler(ArrayList<String> topics, SubscriptionList subscriptions)
        {
            d_topics = topics;
            d_subscriptions = subscriptions;
        }

        public void processEvent(Event event, Session session)
        {
            try {
                switch (event.eventType().intValue())
                {
                    case Event.EventType.Constants.SUBSCRIPTION_DATA:
                        processSubscriptionDataEvent(event, session);
                        break;
                    case Event.EventType.Constants.SUBSCRIPTION_STATUS:
                        synchronized (d_lock) {
                            processSubscriptionStatus(event, session);
                        }
                        break;
                    case Event.EventType.Constants.ADMIN:
                        synchronized (d_lock) {
                            processAdminEvent(event, session);
                        }
                        break;

                    default:
                        processMiscEvents(event, session);
                        break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private boolean processSubscriptionStatus(Event event, Session session)
                throws Exception
        {
            System.out.println("Processing SUBSCRIPTION_STATUS: ");
            SubscriptionList subscriptionList = null;
            for(Message msg: event){
                CorrelationID cid = msg.correlationID();
                String topic = d_topics.get((int)cid.value() - 1);
                System.out.printf(
                        "%s: %s%n",
                        d_dateFormat.format(Calendar.getInstance().getTime()),
                        topic);
                System.out.println("MESSAGE: " + msg);

                if (msg.messageType() == SUBSCRIPTION_TERMINATED
                        && d_pendingUnsubscribe.remove(cid)) {
                    // If this message was due to a previous unsubscribe
                    Subscription subscription = getSubscription(cid);
                    if (d_isSlow) {
                        System.out.printf(
                                "Deferring subscription for topic = %s because session is slow.%n",
                                topic);
                        d_pendingSubscriptions.add(subscription);
                    }
                    else {
                        if (subscriptionList == null) {
                            subscriptionList = new SubscriptionList();
                        }
                        subscriptionList.add(subscription);
                    }
                }
            }

            if (subscriptionList != null && !d_isStopped) {
                session.subscribe(subscriptionList, d_identity);
            }
            return true;
        }

        private boolean processSubscriptionDataEvent(Event event, Session session)
                throws Exception
        {
            System.out.println("Processing SUBSCRIPTION_DATA");
            for(Message msg: event){
                String topic = d_topics.get((int)msg.correlationID().value() - 1);
                System.out.printf(
                        "%s: %s%n",
                        d_dateFormat.format(Calendar.getInstance().getTime()),
                        topic);
                int numFields = msg.asElement().numElements();
                for (int i = 0; i < numFields; ++i) {
                    Element field = msg.asElement().getElement(i);
                    if (field.isNull()) {
                        System.out.println("\t\t" + field.name() + " is NULL");
                        continue;
                    }

                    processElement(field);
                }
                System.out.println();
            }
            return true;
        }

        private void processElement(Element element) throws Exception
        {
            if (element.isArray())
            {
                System.out.println("\t\t" + element.name());
                // process array
                int numOfValues = element.numValues();
                for (int i = 0; i < numOfValues; ++i)
                {
                    // process array data
                    processElement(element.getValueAsElement(i));
                }
                System.out.println();
            }
            else if (element.numElements() > 0)
            {
                System.out.println("\t\t" + element.name());
                int numOfElements = element.numElements();
                for (int i = 0; i < numOfElements; ++i)
                {
                    // process child elements
                    processElement(element.getElement(i));
                }
            }
            else
            {
                // Assume all values are scalar.
                System.out.printf("%40s : %s\n", element.name(), element.getValueAsString());
            }
        }



        private boolean processAdminEvent(Event event, Session session)
                throws Exception
        {
            System.out.println("Processing ADMIN: ");
            ArrayList<CorrelationID> cidsToCancel = null;
            boolean previouslySlow = d_isSlow;
            for(Message msg: event){
                // An admin event can have more than one messages.
                if (msg.messageType() == SLOW_CONSUMER_WARNING) {
                    System.out.printf("MESSAGE: %s%n", msg);
                    d_isSlow = true;
                }
                else if (msg.messageType() == SLOW_CONSUMER_WARNING_CLEARED) {
                    System.out.printf("MESSAGE: %s%n", msg);
                    d_isSlow = false;
                }
                else if (msg.messageType() == DATA_LOSS) {
                    CorrelationID cid = msg.correlationID();
                    String topic = (String) cid.object();
                    System.out.printf(
                            "%s: %s%n",
                            d_dateFormat.format(Calendar.getInstance().getTime()),
                            topic);
                    System.out.printf("MESSAGE: %s%n", msg);
                    if (msg.hasElement(SOURCE)) {
                        String sourceStr = msg.getElementAsString(SOURCE);
                        if (sourceStr.compareTo("InProc") == 0
                                && !d_pendingUnsubscribe.contains(cid)) {
                            // DataLoss was generated "InProc". This can only happen if
                            // applications are processing events slowly and hence are not
                            // able to keep-up with the incoming events.
                            if (cidsToCancel == null) {
                                cidsToCancel = new ArrayList<CorrelationID>();
                            }
                            cidsToCancel.add(cid);
                            d_pendingUnsubscribe.add(cid);
                        }
                    }
                }
            }

            if (!d_isStopped) {
                if (cidsToCancel != null) {
                    session.cancel(cidsToCancel);
                }
                else if ((previouslySlow && !d_isSlow) && !d_pendingSubscriptions.isEmpty()){
                    // Session was slow but is no longer slow. subscribe to any topics
                    // for which we have previously received SUBSCRIPTION_TERMINATED
                    System.out.printf(
                            "Subscribing to topics - %s%n",
                            getTopicsString(d_pendingSubscriptions));
                    session.subscribe(d_pendingSubscriptions, d_identity);
                    d_pendingSubscriptions.clear();
                }
            }
            return true;
        }

        private boolean processMiscEvents(Event event, Session session)
                throws Exception
        {
            System.out.printf("Processing %s%n", event.eventType());
            for(Message msg: event){
                System.out.printf(
                        "%s: %s%n",
                        d_dateFormat.format(Calendar.getInstance().getTime()),
                        msg.messageType());
                System.out.printf(
                        "%s: %s%n",
                        d_dateFormat.format(Calendar.getInstance().getTime()),
                        msg.toString());
            }
            return true;
        }

        private Subscription getSubscription(CorrelationID cid)
        {
            for (Subscription subscription : d_subscriptions) {
                if (subscription.correlationID().equals(cid)) {
                    return subscription;
                }
            }
            throw new IllegalArgumentException(
                    "No subscription found corresponding to cid = " + cid.toString());
        }

        private String getTopicsString(SubscriptionList list)
        {
            StringBuilder strBuilder = new StringBuilder();
            for (int count = 0; count < list.size(); ++count) {
                Subscription subscription = list.get(count);
                if (count != 0) {
                    strBuilder.append(", ");
                }
                strBuilder.append((String) subscription.correlationID().object());
            }
            return strBuilder.toString();
        }
    }
}