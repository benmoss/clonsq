(ns clonsq.test-utils
  (:require [me.raynes.conch.low-level :as sh])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)))

(defrecord NSQ [nsqd nsqlookupd]
  java.io.Closeable
  (close [this]
    (sh/destroy nsqd)
    (sh/destroy nsqlookupd)))

(defn read-lines
  ([stream] (read-lines stream []))
  ([stream line]
   (lazy-seq
     (let [b (.read stream)]
       (cond
         (or (= -1 b)
             (= 0 b)) nil
         (= \newline (char b)) (cons (apply str (conj line (char b)))
                                     (read-lines stream []))
         :else (read-lines stream (conj line (char b))))))))

(defn wait-for-line [lines regex]
  {:pre [(seq lines)]}
  (when (not (re-find regex (first lines)))
    (recur (next lines) regex)))

(defn create-nsq-procs []
  (let [dir (str (Files/createTempDirectory "nsq-tests" (make-array FileAttribute 0)))
        nsqlookupd (sh/proc "nsqlookupd"
                            "--http-address=0.0.0.0:9161"
                            "--tcp-address=0.0.0.0:9160"
                            :dir dir)
        nsqd (sh/proc "nsqd"
                      "--http-address=0.0.0.0:9151"
                      "--tcp-address=0.0.0.0:9150"
                      "--lookupd-tcp-address=0.0.0.0:9160"
                      :dir dir)
        nsqlookupd-stderr (read-lines (:err nsqlookupd))
        nsqd-stderr (read-lines (:err nsqd))
        nsqs (->NSQ nsqd nsqlookupd)]
    (try (wait-for-line nsqlookupd-stderr #"HTTP: listening")
         (wait-for-line nsqd-stderr #"peer info")
         nsqs
         (catch AssertionError e
           (future (dorun (map println nsqlookupd-stderr)))
           (future (dorun (map println nsqd-stderr)))
           (.close nsqs)
           (throw (ex-info "nsq exited abruptly" {} e))))))
