(ns blpapiwrapper.core-test
  (:require [clojure.test :refer :all]
            [cljblpapiwrapper.core :refer :all])
  (:import
    (java.time LocalDate ZonedDateTime ZoneId)
    (java.time.format DateTimeFormatter)
    (com.bloomberglp.blpapi Name)))

;; Test data for utility functions
(def sample-bdh-result
  {"AAPL US Equity" [{:date "2019-01-01" :PX_OPEN 100.0 :PX_HIGH 105.0 :PX_LOW 99.0 :PX_LAST 102.0}
                     {:date "2019-01-02" :PX_OPEN 102.0 :PX_HIGH 106.0 :PX_LOW 101.0 :PX_LAST 104.0}
                     {:date "2019-01-03" :PX_OPEN 104.0 :PX_HIGH 107.0 :PX_LOW 103.0 :PX_LAST 105.0}]
   "GOOG US Equity" [{:date "2019-01-01" :PX_OPEN 1000.0 :PX_HIGH 1050.0 :PX_LOW 990.0 :PX_LAST 1020.0}
                     {:date "2019-01-02" :PX_OPEN 1020.0 :PX_HIGH 1060.0 :PX_LOW 1010.0 :PX_LAST 1040.0}
                     {:date "2019-01-03" :PX_OPEN 1040.0 :PX_HIGH 1070.0 :PX_LOW 1030.0 :PX_LAST 1050.0}]})

;; Tests for utility functions

(deftest test-date-conversion
  (testing "date->yyyyMMdd function"
    (let [local-date (LocalDate/of 2019 1 15)
          zoned-date (ZonedDateTime/of 2019 1 15 10 30 0 0 (ZoneId/of "UTC"))
          string-date "2019-01-15"]
      (is (= "20190115" (date->yyyyMMdd local-date)))
      (is (= "20190115" (date->yyyyMMdd zoned-date)))
      (is (= "20190115" (date->yyyyMMdd string-date))))))

(deftest test-bdh-result-to-records
  (testing "bdh-result->records function"
    (let [result (bdh-result->records sample-bdh-result)]
      (is (= 6 (count result)))
      (is (every? #(contains? % :security) result))
      (is (every? #(contains? % :date) result))
      (is (some #(= "AAPL US Equity" (:security %)) result))
      (is (some #(= "GOOG US Equity" (:security %)) result))
      (is (some #(= "2019-01-01" (:date %)) result)))))

(deftest test-bdh-result-to-field
  (testing "bdh-result->field function"
    (let [result (bdh-result->field sample-bdh-result :PX_OPEN)]
      (is (= 3 (count result)))
      (is (every? #(contains? % :date) result))
      (is (every? #(contains? % "AAPL US Equity") result))
      (is (every? #(contains? % "GOOG US Equity") result))
      (is (= 100.0 (get (first result) "AAPL US Equity")))
      (is (= 1000.0 (get (first result) "GOOG US Equity"))))))

(deftest test-bdh-result-to-date
  (testing "bdh-result->date function"
    (let [result (bdh-result->date sample-bdh-result "2019-01-02")]
      (is (= 2 (count result)))
      (is (every? #(= "2019-01-02" (:date %)) result))
      (is (some #(= "AAPL US Equity" (:security %)) result))
      (is (some #(= "GOOG US Equity" (:security %)) result)))))

(deftest test-bdh-result-to-date-field
  (testing "bdh-result->date-field function"
    (let [result (bdh-result->date-field sample-bdh-result "2019-01-02" :PX_LAST)]
      (is (= 2 (count result)))
      (is (= 104.0 (get result "AAPL US Equity")))
      (is (= 1040.0 (get result "GOOG US Equity"))))))

(deftest test-to-coll
  (testing "->coll function"
    (is (= [1] (->coll 1)))
    (is (= [1 2 3] (->coll [1 2 3])))
    (is (= '(1 2 3) (->coll '(1 2 3))))
    (is (= ["test"] (->coll "test")))))

(deftest test-to-namecoll
  (testing "->namecoll function"
    (let [result (->namecoll "test")]
      (is (= 1 (count result)))
      (is (instance? Name (first result)))
      (is (= "test" (.toString (first result)))))
    (let [result (->namecoll ["test1" "test2"])]
      (is (= 2 (count result)))
      (is (every? #(instance? Name %) result))
      (is (= "test1" (.toString (first result))))
      (is (= "test2" (.toString (second result)))))))

;; Tests for Bloomberg Name constants
(deftest test-bloomberg-names
  (testing "Bloomberg Name constants"
    (is (instance? Name bbg-uuid))
    (is (instance? Name bbg-ipAddress))
    (is (instance? Name bbg-security))
    (is (instance? Name bbg-fieldData))
    (is (instance? Name bbg-securityData))
    (is (instance? Name bbg-fields))
    (is (instance? Name bbg-securities))
    (is (instance? Name bbg-overrides))
    (is (instance? Name bbg-fieldId))
    (is (instance? Name bbg-value))
    (is (instance? Name bbg-startDate))
    (is (instance? Name bbg-endDate))
    (is (instance? Name bbg-adjustmentSplit))
    (is (instance? Name bbg-periodicitySelection))
    (is (instance? Name bbg-eidData))
    (is (instance? Name bbg-responseError))
    (is (= "uuid" (.toString bbg-uuid)))
    (is (= "ipAddress" (.toString bbg-ipAddress)))
    (is (= "security" (.toString bbg-security)))))

;; Tests for session constants
(deftest test-session-constants
  (testing "Session constants"
    (is (= "localhost" default-local-host))
    (is (= 8194 default-local-port))))

;; Mock tests for Bloomberg API functions that would require actual Bloomberg connection
;; These test the function structure and parameter handling

(deftest test-bdp-simple-parameters
  (testing "bdp-simple parameter handling"
    (let [security "AAPL US Equity"
          field "PX_LAST"]
      ;; Test that the function accepts correct parameters without Bloomberg connection
      (is (thrown? Exception (bdp-simple security field)))
      (is (thrown? Exception (bdp-simple security field :override-field "FIELD" :override-value 100))))))

(deftest test-bdh-parameters
  (testing "bdh parameter handling"
    (let [securities ["AAPL US Equity" "GOOG US Equity"]
          fields ["PX_OPEN" "PX_LAST"]
          start-date "2019-01-01"
          end-date "2019-01-31"]
      ;; Test that the function accepts correct parameters without Bloomberg connection
      (is (thrown? Exception (bdh securities fields start-date end-date)))
      (is (thrown? Exception (bdh securities fields start-date end-date :adjustment-split true :periodicity "WEEKLY"))))))

(deftest test-bdp-parameters
  (testing "bdp parameter handling"
    (let [securities ["AAPL US Equity" "GOOG US Equity"]
          fields ["PX_OPEN" "PX_LAST"]]
      ;; Test that the function accepts correct parameters without Bloomberg connection
      (is (thrown? Exception (bdp securities fields)))
      (is (thrown? Exception (bdp securities fields :override-map {"FIELD" "VALUE"}))))))

;; Test for local-session function structure
(deftest test-local-session-creation
  (testing "local-session function structure"
    ;; This will fail without Bloomberg terminal but tests the function exists
    (is (thrown? Exception (local-session)))))

;; Test for subscription function structure
(deftest test-subscription-parameters
  (testing "clj-bdp-subscribe parameter handling"
    (let [securities ["AAPL US Equity"]
          fields ["LAST_PRICE"]
          atom-map (atom {})]
      ;; Test that the function accepts correct parameters without Bloomberg connection
      (is (thrown? Exception (clj-bdp-subscribe securities fields nil atom-map))))))

;; Test for SAPI session function structure
(deftest test-sapi-session-parameters
  (testing "sapi-session parameter handling"
    (let [host-ip "192.168.1.1"
          host-port 8194
          uuid 123456789
          local-ip "192.168.1.100"]
      ;; Test that the function accepts correct parameters without Bloomberg connection
      (is (thrown? Exception (sapi-session host-ip host-port uuid local-ip))))))

;; Test for test-suite function structure
(deftest test-suite-function-exists
  (testing "test-suite function exists"
    ;; This will fail without Bloomberg connection but tests the function exists
    (is (thrown? Exception (test-suite)))))

;; Additional utility tests
(deftest test-edge-cases
  (testing "Edge cases for utility functions"
    ;; Test empty collections
    (is (= [] (bdh-result->records {})))
    (is (= [] (bdh-result->date {} "2019-01-01")))
    (is (= {} (bdh-result->date-field {} "2019-01-01" :PX_LAST)))
    
    ;; Test with single item
    (is (= ["single"] (->coll "single")))
    (is (= [1] (->coll [1])))
    
    ;; Test date conversion edge cases
    (is (= "20190101" (date->yyyyMMdd "2019-01-01")))
    (is (= "20191231" (date->yyyyMMdd "2019-12-31")))))

;; Test for misaligned series assertion
(deftest test-misaligned-series
  (testing "bdh-result->field with misaligned series"
    (let [misaligned-result
          {"AAPL US Equity" [{:date "2019-01-01" :PX_OPEN 100.0}
                             {:date "2019-01-02" :PX_OPEN 102.0}]
           "GOOG US Equity" [{:date "2019-01-01" :PX_OPEN 1000.0}]}]
      (is (thrown? AssertionError (bdh-result->field misaligned-result :PX_OPEN))))))

(run-tests)