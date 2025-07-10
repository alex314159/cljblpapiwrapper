(ns cljblpapiwrapper.core
  (:gen-class)
  (:require [clojure.tools.logging :as log])
  (:import
    (java.time LocalDate ZonedDateTime)
    (java.time.format DateTimeFormatter)
    (com.bloomberglp.blpapi AuthApplication AuthOptions EventHandler Name Identity CorrelationID Session SessionOptions Subscription SubscriptionList MessageIterator Event$EventType$Constants SessionOptions$ClientMode Event Message Element Request NotFoundException EventQueue)))


;; Useful functions, not Bloomberg add-in dependent ;;

(defn- date->yyyyMMdd
  "This will convert all of LocalDate, ZonedDateTime and yyyy-MM-dd into yyyyMMdd"
  [date]
  (condp = (type date)
    LocalDate (.format date (DateTimeFormatter/ofPattern "yyyyMMdd"))
    ZonedDateTime (.format date (DateTimeFormatter/ofPattern "yyyyMMdd"))
    String (clojure.string/replace date #"-" "")))

(defn bdh-result->records
  "This is useful for e.g. vega-lite display"
  [res]
  (apply concat (for [[k v] res] (mapv #(assoc % :security k) v))))

(defn bdh-result->field
  [res field]
  (assert (apply = (map count (vals res))) "Error, series misaligned!")
  (sort-by :date
           (into [] (for [[d v] (group-by :date (bdh-result->records res))]
                      (into {:date d} (for [r v] [(r :security) (r field)]))))))

(defn bdh-result->date
  [res date]
  (filter #(= (:date %) date) (bdh-result->records res)))

(defn bdh-result->date-field
  [res date field]
  (into {} (for [r (bdh-result->date res date)] [(r :security) (r field)])))

(defn ->coll [x] (if (coll? x) x [x]))
(defn ->namecoll [x] (mapv #(Name. %) (->coll x)))

;; Bloomberg names
(def bbg-uuid (Name. "uuid"))
(def bbg-ipAddress (Name. "ipAddress"))
(def bbg-security (Name. "security"))
(def bbg-fieldData (Name. "fieldData"))
(def bbg-securityData (Name. "securityData"))
(def bbg-fields (Name. "fields"))
(def bbg-securities (Name. "securities"))
(def bbg-overrides (Name. "overrides"))
(def bbg-fieldId (Name. "fieldId"))
(def bbg-value (Name. "value"))
(def bbg-startDate (Name. "startDate"))
(def bbg-endDate (Name. "endDate"))
(def bbg-adjustmentSplit (Name. "adjustmentSplit"))
(def bbg-periodicitySelection (Name. "periodicitySelection"))
(def bbg-eidData (Name. "eidData"))
(def bbg-responseError (Name. "responseError"))

;; Session functions

(def default-local-host "localhost")
(def default-local-port 8194)


;;;;;;;;;;;;;;;;;; WORK IN PROGRESS RE BLOOMBERG IDEAL SOLUTION ;;;;;;;;;;;;;;;;;;
(comment
  (defn print-failed-entitlements
    [failed-entitlements]
    (doseq [i (range (count failed-entitlements))]
      (println (.get failed-entitlements i))))

  (defn distribute-message [^Message msg identities uuids]
    (let [service (.service msg)
          failed-entitlements (atom [])
          securities (.getElement msg ^Name bbg-securityData)
          num-securities (.numValues securities)]
      (println "Processing" num-securities "securities:")
      (doseq [i (range num-securities)]
        (let [security (.getValueAsElement securities i)
              ticker (.getElementAsString security ^Name bbg-security)
              entitlements (if (.hasElement security ^Name bbg-eidData) (.getElement security ^Name bbg-eidData))
              num-users (.size identities)]
          (if (and entitlements (not (.isNull entitlements)) (> (.numValues entitlements) 0))
            (dotimes [j num-users]
              (reset! failed-entitlements [])
              (let [identity (.get identities j)
                    uuid (.get uuids j)]
                (if (.hasEntitlements identity entitlements service failed-entitlements)
                  (do
                    (println "User:" uuid "is entitled to get data for:" ticker)
                    (.print msg System/out))
                  (do
                    (println "User:" uuid "is NOT entitled to get data for:" ticker "- Failed eids:")
                    (print-failed-entitlements failed-entitlements)))))
            (dotimes [j num-users]
              (println "User:" (.get uuids j) "is entitled to get data for:" ticker)))))))

  (defn process-response-event [^Event event identities uuids]
    (doseq [^Message msg event]
      (if (.hasElement msg ^Name bbg-responseError)
        (println msg)
        (distribute-message msg identities uuids))))

  (defn make-session-event-handler [identities uuids user-ips]
    (reify EventHandler
      (processEvent [_ event session]
        (let [event-type (.intValue (.eventType event))]
          (cond
            (some #{event-type} [(.intValue Event$EventType$Constants/SESSION_STATUS)
                                 (.intValue Event$EventType$Constants/SERVICE_STATUS)
                                 (.intValue Event$EventType$Constants/REQUEST_STATUS)
                                 (.intValue Event$EventType$Constants/AUTHORIZATION_STATUS)
                                 (.intValue Event$EventType$Constants/SUBSCRIPTION_STATUS)])
            (try (println event) (catch ^Exception e (println "Exception!!!" (.getMessage e))))
            (some #{event-type} [(.intValue Event$EventType$Constants/RESPONSE)
                                 (.intValue Event$EventType$Constants/PARTIAL_RESPONSE)])
            (try
              (process-response-event event identities uuids)
              (catch Exception e
                (println "Library Exception!!!" (.getMessage e)))))))))

  (defn sapi-session-new-untested
    "SAPI authentication
    - host-ip and host-port are for the server
    - uuid is the UUID of a user who's creating the request and is logged into Bloomberg desktop
    - local-ip is the ip of the user"
    [^String host-ip ^Long host-port ^Long uuid ^String local-ip ^String app-name]
    (let [app-corr-id (CorrelationID. app-name)
          auth-options (AuthOptions. (AuthApplication. app-name))
          session-options (doto
                            (SessionOptions.)
                            ;(.setClientMode SessionOptions$ClientMode/SAPI)
                            (.setServerHost host-ip)
                            (.setServerPort host-port)
                            (.setSessionIdentityOptions auth-options app-corr-id))
          session (doto (Session. session-options (make-session-event-handler i u ui)) (.start) (.openService "//blp/apiauth")) ;(SessionEventHandler)
          bbgidentity (.createIdentity session)
          api-auth-svc (.getService session "//blp/apiauth")
          auth-req (doto (.createAuthorizationRequest api-auth-svc) (.set ^Name bbg-uuid (str uuid)) (.set ^Name bbg-ipAddress local-ip))
          corr (CorrelationID. uuid)
          auth-event-queue (EventQueue/new)]
      (.sendAuthorizationRequest session auth-req bbgidentity auth-event-queue corr)
      (loop [s auth-event-queue]
        (let [event (.nextEvent s)]
          (if (= (.intValue (.eventType event)) Event$EventType$Constants/RESPONSE)
            {:session        session
             :success        (.contains (.toString (.next (.messageIterator event))) "AuthorizationSuccess")
             :identity       bbgidentity
             :correlation-id corr}                          ; [session (.contains (.toString (.next (.messageIterator event))) "AuthorizationSuccess")]
            (recur s)))))))

(defn sapi-session
  "SAPI authentication
  - host-ip and host-port are for the server
  - uuid is the UUID of a user who's creating the request and is logged into Bloomberg desktop
  - local-ip is the ip of the user"
  [^String host-ip ^Long host-port ^Long uuid ^String local-ip]
  (let [session-options (doto
                          (SessionOptions.)
                          (.setClientMode SessionOptions$ClientMode/AUTO)
                          (.setServerHost host-ip)
                          (.setServerPort host-port))
        session (doto (Session. session-options) (.start) (.openService "//blp/apiauth"))
        bbgidentity (.createIdentity session)
        api-auth-svc (.getService session "//blp/apiauth")
        auth-req (doto (.createAuthorizationRequest api-auth-svc) (.set ^Name bbg-uuid (str uuid)) (.set ^Name bbg-ipAddress local-ip))
        corr (CorrelationID. "authCorrelation")             ;uuid
        auth-event-queue (EventQueue/new)]
    (.sendAuthorizationRequest session auth-req bbgidentity auth-event-queue corr)
    (loop [s auth-event-queue]
      (let [event (.nextEvent s)]
        (if (= (.intValue (.eventType event)) Event$EventType$Constants/RESPONSE)
          {:session session
           :success (.contains (.toString (.next (.messageIterator event))) "AuthorizationSuccess")
           :identity bbgidentity
           :correlation-id corr
           :session-options session-options}
          (recur s))))))

(defn local-session []
  (doto
    (Session.
      (doto (SessionOptions.)
        (.setServerHost default-local-host)
        (.setServerPort default-local-port)))
    (.start)))

;; Response handling ;;

(defn- handle-response-event [^Event event]
  (log/debug "blp response event" (str event))
  (last (iterator-seq (.messageIterator event))))

(defn- handle-other-event [event] (log/debug "blp other event" (str event)))

(defn- read-spot-response
  "Returns {sec1 {field1 value1 field2 value2} {sec2 {field1 value1 field2 value2}"
  [message fields]
  (let [msg (.getElement ^Message message ^Name bbg-securityData)]
    (into {} (for [secid (range (.numValues msg)) :let [o (.getValueAsElement msg secid) fieldres (.getElement o ^Name bbg-fieldData)]]
               [(.getValueAsString (.getElement o ^Name bbg-security))
                (into {} (for [f (->coll fields) :let [v (.getElement ^Element fieldres ^Name (Name. f))]]
                           [(keyword f) (if (zero? (.numValues v)) nil (.getValueAsString v))]))]))))

(defn- read-historical-response
  "Returns {security [{field1 value1 field2 value2 :date date-id}}"
  [^Message message fields]
  (let [blparray (.getElement (.getElement message ^Name bbg-securityData) ^Name bbg-fieldData)]
    {(.getValueAsString (.getElement (.getElement message ^Name bbg-securityData) ^Name bbg-security) 0)
     (into [] (for [i (range (.numValues blparray)) :let [x (.getValueAsElement blparray i)]]
                (into {:date (.getElementAsString x (Name. "date"))}
                      (for [f fields] [(keyword f) (try (.getElementAsFloat64 x ^Name (Name. f)) (catch NotFoundException e nil))]))))}))

(defn- wait-for-response
  "This will loop indefinitely if no more events"
  [session spot-or-history fields]
  (letfn [(assoc-response [acc event fields]
            (let [response ((if (= spot-or-history :spot) read-spot-response read-historical-response) (handle-response-event event) fields)]
              (merge acc response)))]
    (loop [s session acc {}]
      (let [event (.nextEvent s)]
        (condp = (.intValue (.eventType event))
          Event$EventType$Constants/RESPONSE (assoc-response acc event fields)
          Event$EventType$Constants/PARTIAL_RESPONSE (recur s (assoc-response acc event fields)) ; (assoc-response acc event fields)
          (do (handle-other-event event) (recur s acc)))))))


;; BDP definition ;;

(defn clj-bdp-session
  "We either take the session as an input (SAPI) or create a
  local session, which will only work locally on a computer that is connected to Bloomberg"
  ([securitiescoll fieldscoll override-map session-object]
   (let [session (or (:session session-object) (local-session))
         identity (or (:identity session-object) (.createIdentity session))]
     (.openService session "//blp/refdata")
     (let [request-id (CorrelationID. (rand-int 1000))
           ref-data-service (.getService session "//blp/refdata")
           request (.createRequest ref-data-service "ReferenceDataRequest")]
       (doseq [s securitiescoll] (.append ^Request request ^Name bbg-securities ^String s))
       (doseq [f fieldscoll] (.append ^Request request ^Name bbg-fields ^String f))
       (when override-map
         (doseq [[k v] override-map]
           (doto (.appendElement (.getElement request ^Name bbg-overrides))
             (.setElement ^Name bbg-fieldId ^String k)
             (.setElement ^Name bbg-value v))))
       (.sendRequest ^Session session ^Request request ^Identity identity ^CorrelationID request-id)
       session))))


(defn bdp
  "(bdp [\"AAPL US Equity\" ] [ \"PX_LAST\"] :session-map s)"
  [securities fields & {:keys [session-map override-map] :or {session-map nil override-map nil}}]
  (let [fieldscoll (mapv name (->coll fields))
        new-session (clj-bdp-session (->coll securities) fieldscoll override-map session-map)]
    (wait-for-response new-session :spot fieldscoll)))

(defn bdp-simple
  "One security and one field, one override; will return a string"
  [security field & {:keys [override-field override-value] :or {override-field nil override-value nil}}]
  (get-in
    (if (and override-field override-value)
      (bdp security field :override-map {override-field override-value})
      (bdp security field))
    [security (keyword field)]))


;; BDH definition ;;

(defn- clj-bdh-session
  [securitiescoll fieldscoll start-date end-date adjustment-split periodicity session-input]
  (let [session (or (:session session-input) (local-session))]
    (.openService session "//blp/refdata")
    (let [request-id (CorrelationID. 1)
          ref-data-service (.getService session "//blp/refdata")
          request (doto
                    (.createRequest ref-data-service "HistoricalDataRequest")
                    (.set ^Name bbg-startDate ^String start-date)
                    (.set ^Name bbg-endDate ^String end-date)
                    (.set ^Name bbg-adjustmentSplit (if adjustment-split "TRUE" "FALSE"))
                    (.set ^Name bbg-periodicitySelection ^String periodicity))]
      (doseq [s securitiescoll] (.append request ^Name bbg-securities ^String s))
      (doseq [f fieldscoll] (.append request ^Name bbg-fields ^String f))
      (if session-input
        (.sendRequest ^Session session ^Request request ^Identity (:identity session-input) ^CorrelationID request-id)
        (.sendRequest session request request-id))
      session)))

(defn bdh
  [securities fields start-date end-date & {:keys [adjustment-split periodicity session] :or {adjustment-split false periodicity "DAILY" session nil}}]
  (let [securitiescoll (->coll securities)
        fieldscoll (map name (->coll fields))]
    (wait-for-response (clj-bdh-session securitiescoll fieldscoll (date->yyyyMMdd start-date) (date->yyyyMMdd end-date) adjustment-split periodicity session) :history fieldscoll)))


;Examples
(defn test-suite []
  (let [out1 (bdh ["AAPL US Equity" "GOOG US Equity" "META US Equity"] ["PX_OPEN" "PX_HIGH" "PX_LOW" "PX_LAST"] "20190101" "20190120")
        out2 (bdh ["AAPL US Equity" "GOOG US Equity" "META US Equity"] ["PX_OPEN" "PX_HIGH" "PX_LOW" "PX_LAST"] "20190101" "20190120" :adjustment-split true :periodicity "WEEKLY")
        out3 (bdp-simple "AAPL US Equity" "PX_LAST")
        out4 (bdp-simple "US900123AL40 Corp" "YAS_BOND_YLD" :override-field "YAS_BOND_PX" :override-value 100.)
        out4bis (bdp ["XS1713469911 Corp"] ["BETA_ADJ_OVERRIDABLE"] :override-map {"BETA_OVERRIDE_REL_INDEX" "JBCDCOMP Index" "BETA_OVERRIDE_PERIOD" "D" "BETA_OVERRIDE_START_DT", "20210101"})
        out5 (bdp ["AAPL US Equity" "GOOG US Equity" "META US Equity"] ["PX_OPEN" "PX_HIGH" "PX_LOW" "PX_LAST"])
        out6 (bdh-result->field out1 :PX_OPEN)
        out7 (bdh-result->date out1 "2019-01-18")
        out8 (bdh-result->date-field out1 "2019-01-18" :PX_OPEN)
        out9 (bdh-result->records out1)]
    {:bdh out1
     :bdh-weekly out2
     :bdp-simple out3
     :bdp-simple-override-1 out4
     :bdp-overide out4bis
     :bdp out5
     :bdh-field out6
     :bdh-date out7
     :bdh-date-field out8
     :bdh-records out9}))



;; Subscription ;;

(defn clj-bdp-subscribe
  "This will subscribe to a list of securities and fields and update an atom-map with the values"
  [securities fields session-input atom-map]
  (let [session (or (:session session-input) (local-session))]
    (.openService session "//blp/mktdata")
    (let [subscriptions (SubscriptionList.)
          securitiescoll (->coll securities)
          fieldscoll (->coll fields)
          fieldscollname (->namecoll fieldscoll)
          corrmap (into {} (map-indexed vector securitiescoll))]
      (doseq  [[c s] corrmap]
        (.add subscriptions (Subscription. ^String s (clojure.string/join "," fieldscoll) (CorrelationID. c))))
      (Thread.
        (fn []
          (try
            (if session-input
              (.subscribe ^Session session ^SubscriptionList subscriptions ^Identity (:identity session-input))
              (.subscribe session subscriptions))
            (while true
              (let [event (.nextEvent session)]
                (if (= (.intValue (.eventType event)) Event$EventType$Constants/SUBSCRIPTION_DATA)
                  (let [iter (.messageIterator event) msg (.next iter) s (corrmap (.object (.correlationID msg)))]
                    (doseq [f fieldscollname]
                      (when (.hasElement msg ^Name f) (swap! atom-map assoc-in [s f]  (.getValueAsString (.getElement msg ^Name f)))))))))
          (catch InterruptedException e
            (.stop ^Session session)
            (println (.getMessage e)))))))))



;Examples
;(def m (atom nil))
;(def t (clj-bdp-subscribe ["ESM2 Index" "VGM2 Index"] ["LAST_PRICE"] nil m))
;(.start t)
;;(log/info @m)
;(.interrupt t)