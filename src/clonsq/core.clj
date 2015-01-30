(ns clonsq.core
  (:require [aleph.http :as http]
            [aleph.tcp :as tcp]
            [byte-streams :as bs]
            [cheshire.core :as json]
            [clonsq.protocol :as proto]
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

(defn response-handler [response-stream msg]
  (condp = (get-in msg [:data :body])
    "_heartbeat_" (do (s/put! response-stream (proto/encode :nop))
                      (prn "heartbeat" msg))
    "OK" (prn "OK", msg)
    (prn "Unexpected message" msg)))

(defn data-type= [t]
  (fn [msg] (-> msg
                (get-in [:data :type])
                (= t))))

(defn connect [{:keys [lookupd-http-address topic channel max-in-flight handler]}]
  (d/let-flow [lookup-response (lookup lookupd-http-address topic)
               producer (get-in lookup-response ["data" "producers" 0])
               tcp-stream (tcp/client {:host (get producer "broadcast_address")
                                       :port (get producer "tcp_port")})
               decoded-input-stream (s/map proto/decode tcp-stream)
               responses (s/filter (data-type= :response) decoded-input-stream)
               messages (->> decoded-input-stream
                             (s/filter (data-type= :message))
                             (s/map #(dissoc (:data %) :type)))
               errors (s/filter (data-type= :error) decoded-input-stream)]
    (s/put! tcp-stream (proto/encode :magic-id))
    (s/put! tcp-stream (proto/encode :subscribe topic channel))
    (s/put! tcp-stream (proto/encode :ready max-in-flight))
    (s/consume (partial response-handler tcp-stream) responses)
    (s/consume err-handler errors)
    (s/consume (partial handler tcp-stream) messages)
    tcp-stream))

(defn finish [msg conn]
  (s/put! conn (proto/encode :fin (:id msg))))
