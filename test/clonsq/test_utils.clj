(ns clonsq.test-utils
  (:require [me.raynes.conch.low-level :as sh]
            [plumbing.core :refer [defnk]])
  (:import (java.io Closeable)
           (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)))

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

(defrecord Proc [proc]
  Closeable
  (close [_] (sh/destroy proc)))

(defnk create-nsqlookupd [http-address tcp-address]
  (let [dir (str (Files/createTempDirectory "nsq-tests" (make-array FileAttribute 0)))
        proc (->Proc (sh/proc "nsqlookupd"
                              (str "--http-address=" http-address)
                              (str "--tcp-address=" tcp-address)
                              :dir dir))
        stderr (read-lines (-> proc :proc :err))]
    (try (wait-for-line stderr #"HTTP: listening")
         (catch AssertionError e
           (.close proc)
           (future (dorun (map println stderr)))
           (throw (ex-info "nsqlookupd exited abruptly" {} e))))
    proc))

(defnk create-nsqd [http-address tcp-address lookupd-tcp-address]
  (let [dir (str (Files/createTempDirectory "nsq-tests" (make-array FileAttribute 0)))
        proc (->Proc (sh/proc "nsqd"
                              (str "--http-address=" http-address)
                              (str "--tcp-address=" tcp-address)
                              (str "--lookupd-tcp-address=" lookupd-tcp-address)
                              :dir dir))
        stderr (read-lines (-> proc :proc :err))]
    (try (wait-for-line stderr #"peer info")
         (catch AssertionError e
           (future (dorun (map println stderr)))
           (.close proc)
           (throw (ex-info "nsqd exited abruptly" {} e))))
    proc))
