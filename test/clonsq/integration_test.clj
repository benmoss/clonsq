(ns clonsq.integration-test
  (:require [byte-streams :as bs]
            [clojure.test :refer [deftest is testing]]
            [clonsq.core :as nsq]
            [clonsq.test-utils :refer [create-nsq-procs]]
            [me.raynes.conch.low-level :as sh]))

(deftest receive-a-message
  (testing "a message can be received through a consumer"
    (with-open [nsqs (create-nsq-procs)]
      (is (= 0 (sh/exit-code (sh/proc "curl" "-dhello world" "http://0.0.0.0:9151/put?topic=test"))))
      (let [results (atom [])
            lock (promise)
            consumer (nsq/connect {:lookupd-http-address "http://0.0.0.0:9161"
                                   :topic "test"
                                   :channel "test"
                                   :max-in-flight 200
                                   :handler (fn [sink msg]
                                              (swap! results conj (bs/to-string (:body msg)))
                                              (nsq/finish! sink (:id msg))
                                              (deliver lock true))})]
        @lock
        (is (= @results ["hello world"]))))))
