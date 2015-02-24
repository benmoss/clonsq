(ns clonsq.integration-test
  (:require [byte-streams :as bs]
            [clojure.test :refer [deftest is testing]]
            [clonsq.consumer :as consumer]
            [clonsq.test-utils :refer [create-nsqd create-nsqlookupd]]
            [me.raynes.conch.low-level :as sh]))

(def nsqlookupd-defaults {:http-address "0.0.0.0:9161"
                          :tcp-address "0.0.0.0:9160"})

(def nsqd-defaults {:http-address "0.0.0.0:9151"
                    :tcp-address "0.0.0.0:9150"
                    :lookupd-tcp-address "0.0.0.0:9160"})

(def consumer-defaults
  {:lookupd-http-address (str "http://" (:http-address nsqlookupd-defaults))
   :topic "test"
   :channel "test#ephemeral"
   :max-in-flight 200
   :lookupd-poll-interval 1000
   :handler (fn [sink msg])})

(deftest receive-a-message
  (testing "a message can be received through a consumer"
    (with-open [nsqlookupd (create-nsqlookupd nsqlookupd-defaults)
                nsqd (create-nsqd nsqd-defaults)]
      (is (= 0 (sh/exit-code (sh/proc "curl" "-dhello world" (str "http://"
                                                                  (:http-address nsqd-defaults)
                                                                  "/put?topic=test")))))
      (let [results (atom [])
            lock (promise)
            consumer (consumer/create (assoc consumer-defaults
                                             :handler (fn [sink msg]
                                                        (swap! results conj (bs/to-string (:body msg)))
                                                        (deliver lock true))))]
        (is (deref lock 200 false) "timeout")
        (is (= @results ["hello world"]))
        (consumer/close! consumer)))))

(deftest polling-nsqlookupds
  (testing "a consumer will poll its nsqlookups for new publishers"
    (with-open [nsqlookupd (create-nsqlookupd nsqlookupd-defaults)]
      (let [results (atom [])
            lock (promise)
            consumer (consumer/create (assoc consumer-defaults
                                             :handler (fn [sink msg]
                                                        (swap! results conj (bs/to-string (:body msg)))
                                                        (deliver lock true))
                                             :lookupd-poll-interval 10))]
        (with-open [nsqd (create-nsqd nsqd-defaults)]
          (is (= 0 (sh/exit-code (sh/proc "curl" "-dshibboleth" (str "http://" (:http-address nsqd-defaults) "/put?topic=test")))))
          (is (deref lock 200 false) "timeout")
          (is (= @results ["shibboleth"]))
          (consumer/close! consumer))))))
