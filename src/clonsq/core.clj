(ns clonsq.core
  (:require [aleph.http :as http]
            [aleph.tcp :as tcp]
            [byte-streams :as bs]
            [cheshire.core :as json]
            [clonsq.protocol :as proto]
            [clonsq.consumer :as c]
            [manifold.deferred :as d]
            [manifold.stream :as s])
  (:import (io.netty.buffer ByteBuf)))


(defn lookup [lookupd-host topic]
  (d/chain (http/get (str lookupd-host "/lookup?topic=" topic))
           :body
           bs/to-string
           json/parse-string))

(defn err-handler [msg]
  (prn "ERROR" msg))

(defn response-handler [sink msg]
  (condp = (:body msg)
    "_heartbeat_"  (s/put! sink (proto/encode :nop))
    "OK" nil
    (prn "Unexpected message" msg)))

(defn subscribe-handlers [consumer]
  (doseq [conn @(:connections consumer)]
    (let [{:keys [sink error message response]} (:streams conn)]
      (s/consume (partial response-handler sink) response)
      (s/consume err-handler error)
      (s/consume (partial (:handler consumer) sink) message))))

(defn connect [{:keys [lookupd-http-address topic] :as opts}]
  (let [lookup-response @(lookup lookupd-http-address topic)
        producers (get-in lookup-response ["data" "producers"])
        consumer (c/create producers opts)]
    (subscribe-handlers consumer)
    consumer))

(defn finish! [msg sink]
  (s/put! sink (proto/encode :fin (:id msg))))

(defn close! [consumer]
  (doseq [conn @(:connections consumer)
          :let [streams (:streams conn)]]
    (s/put! (:sink streams) (proto/encode :close))
    (dorun (map #(s/close! %) (vals streams)))))

(comment
  (defn handler [sink msg]
    (prn msg)
    (finish! msg sink))

  (def consumer (connect {:lookupd-http-address "http://localhost:4161"
                          :topic "test"
                          :channel "test"
                          :max-in-flight 200
                          :handler handler}))
  )
