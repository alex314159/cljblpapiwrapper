(defproject cljblpapiwrapper "0.1.1-SNAPSHOT"
  :description "Simple Clojure wrapper around the Bloomberg Java API"
  :url "https://github.com/alex314159/cljblpapiwrapper"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [alex314159/bberg-sdk "3.8.8.2.1"]]
  :repl-options {:init-ns cljblpapiwrapper.core})
