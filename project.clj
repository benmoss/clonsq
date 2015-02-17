(defproject clonsq "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [aleph "0.4.0-SNAPSHOT"]
                 [cheshire "5.4.0"]
                 [gloss "0.2.4"]
                 [prismatic/plumbing "0.3.7"]]
   :profiles {:dev {:resource-paths ["examples"]
                    :dependencies [[org.clojure/tools.cli "0.3.1"]
                                   [me.raynes/conch "0.8.0"]
                                   [org.clojure/tools.trace "0.7.8"]]}})
