(defproject cljblpapiwrapper "0.3.6"
  :description "Simple Clojure wrapper for the Bloomberg Java API"
  :url "https://github.com/alex314159/cljblpapiwrapper"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [alex314159/bberg-sdk "3.25.2.1.2"]]
  :java-source-paths ["src/cljblpapiwrapper"]
  :repl-options {:init-ns cljblpapiwrapper.core})
