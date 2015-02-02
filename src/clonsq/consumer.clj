(ns clonsq.consumer
  (:require [aleph.tcp :as tcp]
            [clonsq.protocol :as proto]
            [manifold.deferred :as d]
            [manifold.stream :as s]))

(defn tcp-client [{:strs [broadcast_address tcp_port]}]
  (tcp/client {:host broadcast_address
               :port tcp_port}))

(defn create [producers {:keys [topic channel max-in-flight]}]
  (let [connections (map (comp deref tcp-client) producers)]
    (doseq [conn connections]
      (s/put! conn (proto/encode :magic-id))
      (s/put! conn (proto/encode :subscribe topic channel))
      (s/put! conn (proto/encode :ready max-in-flight)))
    {:connections connections}))

(defn- substream [stream t]
  (s/filter #(= t (:type %)) stream))

(defn split-stream [stream]
  (let [decoded-stream (proto/decode-stream stream)]
    {:sink (s/sink-only stream)
     :response (substream decoded-stream :response)
     :message (substream decoded-stream :message)
     :error (substream decoded-stream :error)}))

(defn streams [{:keys [connections] :as consumer}]
  (map split-stream connections))
