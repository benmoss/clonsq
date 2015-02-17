(ns clonsq.consumer-test
  (:require [clojure.test :refer :all]
            [clonsq.consumer :as c]
            [manifold.stream :as s]))

(deftest rdy-maintenance
  (let [consumer {:max-in-flight (atom 10)
                  :connections (atom (range 2))}]
    (testing "when the connection rdy is <= 1
             it resets it to per-conn-max-in-flight"
      (let [connection {:rdy (atom 1)
                        :last-rdy (atom 1)
                        :streams {:sink (s/stream)}}]
        (c/update-rdy consumer connection {})
        (is (= 5 @(:rdy connection)
                 @(:last-rdy connection)))))

    (testing "when the connection rdy is at 25% of its last value
             it resets it to per-conn-max-in-flight"
      (let [connection {:rdy (atom 2)
                        :last-rdy (atom 8)
                        :streams {:sink (s/stream)}}]
        (c/update-rdy consumer connection {})
        (is (= 5 @(:rdy connection)
                 @(:last-rdy connection)))))

    (testing "when the connection rdy is > the per-conn-max-in-flight
             it resets it to per-conn-max-in-flight"
      (let [connection {:rdy (atom 80)
                        :last-rdy (atom 80)
                        :streams {:sink (s/stream)}}]
        (c/update-rdy consumer connection {})
        (is (= 5 @(:rdy connection)
                 @(:last-rdy connection)))))

    (testing "when the connection rdy is > 1
             it decrements it"
      (let [connection {:rdy (atom 5)
                        :last-rdy (atom 5)
                        :streams {:sink (s/stream)}}]
        (c/update-rdy consumer connection {})
        (is (= 4 @(:rdy connection)))
        (is (= 5 @(:last-rdy connection)))))))

(deftest per-conn-max-in-flight
  (testing "with a max-in-flight greater than conn-count
           it is max-in-flight / conn-count"
    (is (= (c/per-conn-max-in-flight {:max-in-flight (atom 50)
                                      :connections (atom (range 15))})
           (int (/ 50 15)))))

  (testing "with a max-in-flight less than conn-count
           it is 1"
    (is (= (c/per-conn-max-in-flight {:max-in-flight (atom 1)
                                      :connections (atom (range 100))})
           1)))
  (testing "with no connections
           it is max-in-flight"
    (is (= (c/per-conn-max-in-flight {:max-in-flight (atom 10)
                                      :connections (atom [])})
           10))))

(deftest connections-timeout
  (testing "connecting to a nonexistent nsqlookupd will timeout"
    (is (= "error querying nsqlookupd (http://0.0.0.0:9999/lookup?topic=foo)\n"
           (with-out-str
             (c/close! (c/create {:lookupd-http-address "http://0.0.0.0:9999"
                        :handler (fn [_ _])
                        :max-in-flight 200
                        :lookupd-poll-interval 2000
                        :channel "foo"
                        :topic "foo"})))))))
