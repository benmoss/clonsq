(ns clonsq.core-test
  (:require [clojure.test :refer :all]
            [clonsq.core :as nsq])
  (:import (java.util.concurrent TimeoutException)))


(deftest connections-timeout
  (testing "connecting to a nonexistent nsqlookupd will timeout"
    (is (= "error querying nsqlookupd (http://0.0.0.0:9999/lookup?topic=foo)\n"
           (with-out-str
             (nsq/connect {:lookupd-http-address "http://0.0.0.0:9999"
                           :topic "foo"}))))))
