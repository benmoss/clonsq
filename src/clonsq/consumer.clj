(ns clonsq.consumer
  (:require [aleph.tcp :as tcp]
            [clonsq.protocol :as proto]
            [manifold.deferred :as d]
            [manifold.stream :as s]))

(defn tcp-client [{:strs [broadcast_address tcp_port]}]
  (tcp/client {:host broadcast_address
               :port tcp_port}))

(defn ->connection [stream]
  {:streams (split-stream stream)
   :rdy (atom 1)
   :last-rdy (atom 1)})

(defn create [producers {:keys [topic channel max-in-flight handler]}]
  (let [connections (map (comp ->connection deref tcp-client) producers)]
    (doseq [conn connections]
      (let [sink (get-in conn [:streams :sink])]
        (s/put! sink (proto/encode :magic-id))
        (s/put! sink (proto/encode :subscribe topic channel))
        (s/put! sink (proto/encode :rdy @(:rdy conn)))))
    {:connections (atom connections)
     :max-in-flight (atom max-in-flight)
     :handler handler}))

(defn- substream [stream t]
  (s/filter #(= t (:type %)) stream))

(defn split-stream [stream]
  (let [decoded-stream (proto/decode-stream stream)]
    {:sink (s/sink-only stream)
     :response (substream decoded-stream :response)
     :message (substream decoded-stream :message)
     :error (substream decoded-stream :error)}))
