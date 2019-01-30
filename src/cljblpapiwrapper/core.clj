(ns cljblpapiwrapper.core
  (:gen-class)
  (:import (com.bloomberglp.blpapi CorrelationID Session SessionOptions)) ;Event Message MessageIterator Request Service
  )




;BDP implementation
(defn- clj-bdp-session [securities fields override-field override-value]
  (let [session-options (doto (SessionOptions.) (.setServerHost "localhost") (.setServerPort 8194))
        session (doto (Session. session-options) (.start) (.openService "//blp/refdata"))
        request-id (CorrelationID. 1)
        securitiescoll (if (coll? securities) securities [securities])
        fieldscoll (if (coll? fields) fields [fields])
        ref-data-service (.getService session "//blp/refdata")
        request (.createRequest ref-data-service "ReferenceDataRequest")]
    (doseq [s securitiescoll] (.append request "securities" s))
    (doseq [f fieldscoll] (.append request "fields" f))
    (when override-field
      (doto (.appendElement (.getElement request "overrides"))
        (.setElement "fieldId" override-field)
        (.setElement "value" override-value)))
    (.sendRequest session request request-id)
    session))

(defn- handle-response-event [event]
  (loop [iter (.messageIterator event)]
    (let [res (.next iter)]
      (if (.hasNext iter) (recur iter) res))))

(defn- handle-other-event [event] nil) ;(println "non-event")

(defn- wait-for-response [session]
  ; will loop indefinitely if no more events
  (loop [s session]
    (let [event (.nextEvent s)]
      (condp = (.intValue (.eventType event))
        com.bloomberglp.blpapi.Event$EventType$Constants/RESPONSE (handle-response-event event)
        com.bloomberglp.blpapi.Event$EventType$Constants/PARTIAL_RESPONSE (do (handle-other-event event) (recur s))
        (do (handle-other-event event) (recur s))))))

(defn- read-spot-response [message fields]
  (let [msg (.getElement message "securityData")
        fieldscoll (if (coll? fields)  fields [fields])]
    (into {} (for [secid (range (.numValues msg))]
               (let [o (.getValueAsElement msg secid)]
                 [(.getValueAsString (.getElement  o "security"))
                  (let [fieldres (.getElement o "fieldData")]
                    (into {} (for [f fieldscoll] [(keyword f)
                                                  (let [v (.getElement fieldres f)]
                                                    (if (zero? (.numValues v)) nil (.getValueAsString v)))])))])))))

(defn bdp [security field & {:keys [override-field override-value] :or {override-field nil override-value nil}}]
  (if (and override-field override-value)
    (read-spot-response
      (wait-for-response
        (clj-bdp-session security field override-field override-value)) field)
    (read-spot-response
      (wait-for-response
        (clj-bdp-session security field nil nil)) field)))


;BDH implementation
(defn- clj-bdh-session [securities fields start-date end-date adjustment-split periodicity]
  (let [session-options (doto (SessionOptions.) (.setServerHost "localhost") (.setServerPort 8194))
        session (doto (Session. session-options) (.start) (.openService "//blp/refdata"))
        request-id (CorrelationID. 1)
        ref-data-service (.getService session "//blp/refdata")
        securitiescoll (if (coll? securities) securities [securities])
        fieldscoll (if (coll? fields) fields [fields])
        request (doto
                  (.createRequest ref-data-service "HistoricalDataRequest")
                  (.set "startDate" start-date)
                  (.set "endDate" end-date)
                  (.set "adjustmentSplit" (if adjustment-split "TRUE" "FALSE"))
                  (.set "periodicitySelection" periodicity))]
    (doseq [s securitiescoll] (.append request "securities" s))
    (doseq [f fieldscoll] (.append request "fields" f))
    (.sendRequest session request request-id)
    session))

(defn- read-historical-response [message fields]
  (let [blparray (.getElement (.getElement message "securityData") "fieldData")
        fieldscoll (if (coll? fields)  fields [fields])]
    [(.getValueAsString (.getElement (.getElement message "securityData") "security") 0)
     (into [] (for [i (range (.numValues blparray))]
                (let [x (.getValueAsElement blparray i)]
                  (assoc (into {} (for [f fieldscoll] [(keyword f) (.getElementAsFloat64 x f)])) :date (.getElementAsString x "date")))))]))

(defn- wait-for-historical-response [session fields]
  ; will loop indefinitely if no more events
  (loop [s session acc {}]
    (let [event (.nextEvent s)]
      (condp = (.intValue (.eventType event))
        com.bloomberglp.blpapi.Event$EventType$Constants/RESPONSE (let [response (read-historical-response (handle-response-event event) fields)]
                                                                    (assoc acc (first response) (second response)))
        com.bloomberglp.blpapi.Event$EventType$Constants/PARTIAL_RESPONSE (let [response (read-historical-response (handle-response-event event) fields)]
                                                                            (recur s (assoc acc (first response) (second response))))
        (do (handle-other-event event) (recur s acc))))))

(defn bdh [securities fields start-date end-date & {:keys [adjustment-split periodicity] :or {adjustment-split false periodicity "DAILY"}}]
    (wait-for-historical-response
      (clj-bdh-session securities fields start-date end-date adjustment-split periodicity) fields))


;Convenience functions

(defn bdp-simple [security field & {:keys [override-field override-value] :or {override-field nil override-value nil}}]
  ;this is used to ask for one security and one field - will return a string
  (get-in (bdp security field :override-field override-field :override-value override-value) [security (keyword field) ]))

(defn field-from-timeseries [bdhm-result field]
  ;warning - this will only give correct results if the different series are aligned - no check for this is implemented
  (let [cross (apply map (cons vector (map #(bdhm-result %) (keys bdhm-result))))
        f (fn [a] [(first a) (field (second a))])]
    (into [] (for [k cross]
               (assoc
                 (into {} (map f (map vector (keys bdhm-result) k)))
                 :date
                 (:date (first k)))))))

(defn date-from-timeseries [m date]
  (into {} (for [s m line (second s)] (if (= (:date line) date) [(first s) line] nil))))

(defn field-date-from-timeseries [bdhm-result field date]
  (let [data (date-from-timeseries bdhm-result date)
        data2 (into {} (for [[a b] data] [a [b]]))]
    (first (field-from-timeseries data2 field))))


;Examples
;(def out1 (bdh ["AAPL US Equity" "GOOG US Equity" "FB US Equity"] ["PX_OPEN" "PX_HIGH" "PX_LOW" "PX_LAST"] "20190101" "20190120"))
;(def out2 (bdh ["AAPL US Equity" "GOOG US Equity" "FB US Equity"] ["PX_OPEN" "PX_HIGH" "PX_LOW" "PX_LAST"] "20190101" "20190120" :adjustment-split true :periodicity "WEEKLY"))
;(def out3 (bdp-simple "AAPL US Equity" "PX_LAST"))
;(def out4 (bdp-simple "US900123AL40 Corp" "YAS_BOND_YLD" :override-field "YAS_BOND_PX" :override-value 100.))
;(def out5 (bdp ["AAPL US Equity" "GOOG US Equity" "FB US Equity"] ["PX_OPEN" "PX_HIGH" "PX_LOW" "PX_LAST"]))
;(def out6 (field-from-timeseries out1 :PX_OPEN))
;(def out7 (date-from-timeseries out1 "2019-01-18"))
;(def out8 (field-date-from-timeseries out1 :PX_OPEN "2019-01-18"))
;


