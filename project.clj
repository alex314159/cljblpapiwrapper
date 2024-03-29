(defproject cljblpapiwrapper "0.3.5"
  :description "Simple Clojure wrapper around the Bloomberg Java API"
  :url "https://github.com/alex314159/cljblpapiwrapper"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [alex314159/bberg-sdk "3.18.0.1.3"]]
  :repl-options {:init-ns cljblpapiwrapper.core})
