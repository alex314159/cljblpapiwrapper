(ns blpapiwrapper.core
  )


(import (com.bloomberglp.blpapi CorrelationID Event Message MessageIterator Request Service Session SessionOptions))

(defn clj-bdp-session [security field override-field override-value]
  (let [session-options (doto (SessionOptions.) (.setServerHost "localhost") (.setServerPort 8194))
        session (doto (Session. session-options) (.start) (.openService "//blp/refdata"))
        request-id (CorrelationID. 1)
        ref-data-service (.getService session "//blp/refdata")
        request (doto
                  (.createRequest ref-data-service "ReferenceDataRequest")
                  (.append "securities" security)
                  (.append "fields" field))]
  (when override-field
    (doto (.appendElement (.getElement request "overrides"))
      (.setElement "fieldId" override-field)
      (.setElement "value" override-value)))
  (.sendRequest session request request-id)
  session))

(defn clj-bdh-session [security fields start-date end-date adjustment-split periodicity]
  (let [session-options (doto (SessionOptions.) (.setServerHost "localhost") (.setServerPort 8194))
        session (doto (Session. session-options) (.start) (.openService "//blp/refdata"))
        request-id (CorrelationID. 1)
        ref-data-service (.getService session "//blp/refdata")
        fieldscoll (if (coll? fields) fields [fields])
        request (doto
                  (.createRequest ref-data-service "HistoricalDataRequest")
                  (.append "securities" security)
                  (.set "startDate" start-date)
                  (.set "endDate" end-date)
                  (.set "adjustmentSplit" (if adjustment-split "TRUE" "FALSE"))
                  (.set "periodicitySelection" periodicity))]
    (doseq [f fieldscoll] (.append request "fields" f))
    (.sendRequest session request request-id)
    session))

(defn handle-response-event [event]
  (loop [iter (.messageIterator event)]
    (let [res (.next iter)]
      (if (.hasNext iter) (recur iter) res))))

(defn handle-other-event [event] nil) ;(println "non-event")

(defn wait-for-response [session]
  ; will loop indefinitely if no more events
  (loop [s session]
    (let [event (.nextEvent s)]
      (condp = (.intValue (.eventType event))
        com.bloomberglp.blpapi.Event$EventType$Constants/RESPONSE (handle-response-event event)
        com.bloomberglp.blpapi.Event$EventType$Constants/PARTIAL_RESPONSE (do (handle-other-event event) (recur s))
        (do (handle-other-event event) (recur s))))))

(defn read-single-response [message field]
  (-> message
      (.getElement "securityData")
      (.getValueAsElement 0)
      (.getElement "fieldData")
      (.getElementAsString field)))

(defn read-historical-response [message fields]
  (let [blparray (.getElement (.getElement message "securityData") "fieldData") fieldscoll (if (coll? fields)  fields [fields])]
    (into [] (for [i (range (.numValues blparray))]
               (let [x (.getValueAsElement blparray i)]
                 (assoc (into {} (for [f fieldscoll] [(keyword f) (.getElementAsFloat64 x f)])) :date (.getElementAsString x "date")))))))

(defn cljbdp [security field & {:keys [override-field override-value] :or {override-field nil override-value nil}}]
  (if (and override-field override-value)
    (read-single-response
      (wait-for-response
        (clj-bdp-session security field override-field override-value)) field)
    (read-single-response
      (wait-for-response
        (clj-bdp-session security field nil nil)) field)))

(defn cljbdh [security fields start-date end-date & {:keys [adjustment-split periodicity] :or {adjustment-split false periodicity "DAILY"}}]
  (read-historical-response
    (wait-for-response
      (clj-bdh-session security fields start-date end-date adjustment-split periodicity)) fields))


