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

(def wtf (atom []))
(defmulti handle-message (fn [msg] (get-in msg [:data :body])))
(defmethod handle-message "_heartbeat_" [_] "NOP\n")
(defmethod handle-message "OK" [_])
(defmethod handle-message :default [msg]
  (swap! wtf conj msg)
  (println "UNKNOWN MESSAGE" msg))

(defmulti handle-input (fn [_ input] (class input)))
(defmethod handle-input ByteBuf [stream buf]
  (when-let [response (handle-message (proto/decode buf))]
    (s/put! stream response)))
(defmethod handle-input :default [stream input]
  (println "Unknown input!" input))

(defn connect [{:strs [broadcast_address tcp_port]}]
  (d/let-flow [stream (tcp/client {:host broadcast_address :port tcp_port})]
    (s/put! stream "  V2")
    stream))

(comment
  (def foo (d/let-flow [response (lookup "http://localhost:4161" "test")
               producer (get-in response ["data" "producers" 0])
               stream (connect producer)]
    (s/consume (partial handle-input stream) stream)
    stream))

  (s/put! @foo "SUB test clonsq \n")
  (s/put! @foo "RDY 1\n")
  (s/put! @foo "CLS\n")
  (d/chain (s/take! stream)
           bs/to-string
           prn)
  (def stream *1)

  ; a lookup response
  {"status_code" 200, "status_txt" "OK", "data" {"channels" [],
                                                 "producers" [{"remote_address" "127.0.0.1:56958",
                                                               "hostname" "fluorine-2.local",
                                                               "broadcast_address" "fluorine-2.local",
                                                               "tcp_port" 4150,
                                                               "http_port" 4151,
                                                               "version" "0.2.30"}]}})
