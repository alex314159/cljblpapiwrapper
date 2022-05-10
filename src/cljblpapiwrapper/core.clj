(ns cljblpapiwrapper.core
  (:gen-class)
  (:import
    (java.time LocalDate ZonedDateTime)
    (java.time.format DateTimeFormatter)
    (com.bloomberglp.blpapi CorrelationID Session SessionOptions Subscription SubscriptionList MessageIterator Event$EventType$Constants SessionOptions$ClientMode Event Message Element Request NotFoundException)))


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


;; Session functions

(def default-local-host "localhost")
(def default-local-port 8194)

(defn sapi-session
  "SAPI authentication
  - host-ip and host-port are for the server
  - uuid is the UUID of a user who's creating the request and is logged into Bloomberg desktop
  - local-ip is the ip of the user"
  [^String host-ip ^Long host-port ^Long uuid ^String local-ip]
  (let [session-options (doto
                          (SessionOptions.)
                          (.setClientMode SessionOptions$ClientMode/SAPI)
                          (.setServerHost host-ip)
                          (.setServerPort host-port))
        session (doto (Session. session-options) (.start) (.openService "//blp/apiauth"))
        bbgidentity (.createIdentity session)
        api-auth-svc (.getService session "//blp/apiauth")
        auth-req (doto (.createAuthorizationRequest api-auth-svc) (.set "uuid" (str uuid)) (.set "ipAddress" local-ip))
        corr (CorrelationID. uuid)]
    (.sendAuthorizationRequest session auth-req bbgidentity corr)
    (loop [s session]
      (let [event (.nextEvent s)]
        (if (= (.intValue (.eventType event)) Event$EventType$Constants/RESPONSE)
          [session (= (subs (.toString (.next (.messageIterator event))) 0 20) "AuthorizationSuccess")]
          (recur s))))))


;; Response handling ;;

(defn- handle-response-event [event]
  (loop [iter (.messageIterator ^Event event)]
    (let [res (.next iter)]
      (if (.hasNext iter) (recur iter) res))))

(defn- handle-other-event [event] nil) ;(log/info "non-event")

(defn- read-spot-response
  "Returns {sec1 {field1 value1 field2 value2} {sec2 {field1 value1 field2 value2}"
  [message fields]
  (let [msg (.getElement ^Message message "securityData") ;;message
        fieldscoll (if (coll? fields) fields [fields])]
    (into {} (for [secid (range (.numValues msg))]
               (let [o (.getValueAsElement msg secid)]
                 [(.getValueAsString (.getElement  o "security"))
                  (let [fieldres (.getElement o "fieldData")]
                    (into {} (for [f fieldscoll] [(keyword f)
                                                  (let [v (.getElement ^Element fieldres f)]
                                                    (if (zero? (.numValues v)) nil (.getValueAsString v)))])))])))))

(defn- read-historical-response
  "Returns {security [{field1 value1 field2 value2 :date date-id}}"
  [message fields]
  (let [blparray (.getElement (.getElement message "securityData") "fieldData")
        fieldscoll (if (coll? fields) fields [fields])]
    {(.getValueAsString (.getElement (.getElement message "securityData") "security") 0)
     (into [] (for [i (range (.numValues blparray))]
                (let [x (.getValueAsElement blparray i)]
                  (into {:date (.getElementAsString x "date")} (for [f fieldscoll] [(keyword f) (try (.getElementAsFloat64 x f) (catch NotFoundException e nil))])))))}))

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
  ([securitiescoll fieldscoll override-map session-input]
   (let [session (if session-input session-input (doto (Session. (doto (SessionOptions.) (.setServerHost default-local-host) (.setServerPort default-local-port))) (.start)))]
     (.openService session "//blp/refdata")
     (let [request-id (CorrelationID. 1)
           ref-data-service (.getService session "//blp/refdata")
           request (.createRequest ref-data-service "ReferenceDataRequest")]
       (doseq [s securitiescoll] (.append ^Request request "securities" s))
       (doseq [f fieldscoll] (.append ^Request request "fields" f))
       (when override-map
         (doseq [[k v] override-map]
           (doto (.appendElement (.getElement request "overrides"))
             (.setElement "fieldId" k)
             (.setElement "value" v))))
       (.sendRequest session request request-id)
       session))))

(defn bdp [securities fields & {:keys [session override-map] :or {session nil override-map nil}}]
  (let [securitiescoll (if (coll? securities) securities [securities])
        fieldscoll (map name (if (coll? fields) fields [fields]))]
    (wait-for-response (clj-bdp-session securitiescoll fieldscoll override-map session) :spot fieldscoll)))

(defn bdp-simple
  "One security and one field, one override- will return a string"
  [security field & {:keys [override-field override-value] :or {override-field nil override-value nil}}]
  (get-in
    (if (and override-field override-value)
      (bdp security field :override-map {override-field override-value})
      (bdp security field))
    [security (keyword field)]))


;; BDH definition ;;

(defn- clj-bdh-session [securitiescoll fieldscoll start-date end-date adjustment-split periodicity session-input]
  (let [session (if session-input session-input (doto (Session. (doto (SessionOptions.) (.setServerHost default-local-host) (.setServerPort default-local-port))) (.start)))]
    (.openService session "//blp/refdata")
    (let [request-id (CorrelationID. 1)
          ref-data-service (.getService session "//blp/refdata")
          request (doto
                    (.createRequest ref-data-service "HistoricalDataRequest")
                    (.set "startDate" start-date)
                    (.set "endDate" end-date)
                    (.set "adjustmentSplit" (if adjustment-split "TRUE" "FALSE"))
                    (.set "periodicitySelection" periodicity))]
      (doseq [s securitiescoll] (.append request "securities" s))
      (doseq [f fieldscoll] (.append request "fields" f))
      (.sendRequest session request request-id)
      session)))

(defn bdh [securities fields start-date end-date & {:keys [adjustment-split periodicity session] :or {adjustment-split false periodicity "DAILY" session nil}}]
  (let [securitiescoll (if (coll? securities) securities [securities])
        fieldscoll (map name (if (coll? fields) fields [fields]))]
    (wait-for-response (clj-bdh-session securitiescoll fieldscoll (date->yyyyMMdd start-date) (date->yyyyMMdd end-date) adjustment-split periodicity session) :history fieldscoll)))


;Examples
;(def out1 (bdh ["AAPL US Equity" "GOOG US Equity" "FB US Equity"] ["PX_OPEN" "PX_HIGH" "PX_LOW" "PX_LAST"] "20190101" "20190120"))
;(def out2 (bdh ["AAPL US Equity" "GOOG US Equity" "FB US Equity"] ["PX_OPEN" "PX_HIGH" "PX_LOW" "PX_LAST"] "20190101" "20190120" :adjustment-split true :periodicity "WEEKLY"))
;(def out3 (bdp-simple "AAPL US Equity" "PX_LAST"))
;(def out4 (bdp-simple "US900123AL40 Corp" "YAS_BOND_YLD" :override-field "YAS_BOND_PX" :override-value 100.))
;(def out4bis (bdp ["XS1713469911 Corp"] ["BETA_ADJ_OVERRIDABLE"] :override-map {"BETA_OVERRIDE_REL_INDEX" "JBCDCOMP Index" "BETA_OVERRIDE_PERIOD" "D"  "BETA_OVERRIDE_START_DT","20210101"}))
;(def out5 (bdp ["AAPL US Equity" "GOOG US Equity" "FB US Equity"] ["PX_OPEN" "PX_HIGH" "PX_LOW" "PX_LAST"]))
;(def out6 (bdh-result->field out1 :PX_OPEN))
;(def out7 (bdh-result->date out1 "2019-01-18+00:00"))
;(def out8 (bdh-result->date-field out1 "2019-01-18+00:00" :PX_OPEN))
;(def out9 (bdh-result->records out1))



;; Subscription ;;

(defn clj-bdp-subscribe [securities fields session-input atom-map]
  (let [session (if session-input session-input (doto (Session. (doto (SessionOptions.) (.setServerHost default-local-host) (.setServerPort default-local-port))) (.start)))]
    (.openService session "//blp/mktdata")
    (let [subscriptions (SubscriptionList.)
          securitiescoll (if (coll? securities) securities [securities])
          fieldscoll (if (coll? fields) fields [fields])
          fieldstring (clojure.string/join "," fieldscoll)
          corrmap (into {} (map-indexed vector securitiescoll))]
      (doseq  [[c s] corrmap]
        (.add subscriptions (Subscription. s fieldstring (CorrelationID. c))))
      (Thread.
        (fn []
          (try
            (.subscribe session subscriptions)
            (while true
              (let [event (.nextEvent session)]
                (if (= (.intValue (.eventType event)) Event$EventType$Constants/SUBSCRIPTION_DATA)
                  (let [iter (.messageIterator event) msg (.next iter) s (corrmap (.object (.correlationID msg)))]
                    (doseq [f fieldscoll]
                      (when (.hasElement msg f) (swap! atom-map assoc-in [s f]  (.getValueAsString (.getElement msg f)))))))))
          (catch InterruptedException e
            (.stop session)
            (println (.getMessage e)))))))))



;Examples
;(def m (atom nil))
;(def t (clj-bdp-subscribe ["ESM2 Index" "VGM2 Index"] ["LAST_PRICE"] nil m))
;(.start t)
;;(log/info @m)
;(.stop t)
