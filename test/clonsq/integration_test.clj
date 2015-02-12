(ns clonsq.integration-test
  (:require [byte-streams :as bs]
            [clojure.test :refer [deftest is testing]]
            [clonsq.core :as nsq]
            [me.raynes.conch.low-level :as sh])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)))

(defrecord NSQ [nsqd nsqlookupd]
  java.io.Closeable
  (close [this]
    (sh/destroy nsqd)
    (sh/destroy nsqlookupd)))

(defn create-nsq-procs []
  (let [dir (str (Files/createTempDirectory "nsq-tests" (make-array FileAttribute 0)))
        nsqlookupd (sh/proc "nsqlookupd"
                            "--http-address=0.0.0.0:9161"
                            "--tcp-address=0.0.0.0:9160"
                            "--verbose"
                            :dir dir)
        nsqd (sh/proc "nsqd"
                      "--http-address=0.0.0.0:9151"
                      "--tcp-address=0.0.0.0:9150"
                      "--lookupd-tcp-address=0.0.0.0:9160"
                      "--verbose"
                      :dir dir)]
    (->NSQ nsqd nsqlookupd)))

(defn read-lines
  ([stream] (read-lines stream []))
  ([stream line]
   (lazy-seq
     (let [b (.read stream)]
       (cond
         (= -1 (int b)) nil
         (= \newline (char b)) (cons (apply str (conj line (char b)))
                                     (read-lines stream []))
         :else (read-lines stream (conj line (char b))))))))

(defn wait-for-line [lines regex]
  (when (not (re-find regex (first lines)))
    (recur (next lines) regex)))

(defn wait-for-boot [{:keys [nsqd nsqlookupd]}]
  (let [lookup-lines (read-lines (:err nsqlookupd))
        nsqd-lines (read-lines (:err nsqd))]
    (wait-for-line lookup-lines #"HTTP: listening")
    (wait-for-line nsqd-lines #"peer info")))

(deftest receive-a-message
  (testing "a message can be received through a consumer"
    (with-open [nsqs (create-nsq-procs)]
      (wait-for-boot nsqs)
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
