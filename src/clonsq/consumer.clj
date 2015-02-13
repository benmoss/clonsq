(ns clonsq.consumer
  (:require [aleph.http :as http]
            [aleph.tcp :as tcp]
            [byte-streams :as bs]
            [cheshire.core :as json]
            [clonsq.protocol :as proto]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [plumbing.core :refer [defnk]])
  (:import (java.util.concurrent TimeoutException)))

(defnk per-conn-max-in-flight [max-in-flight connections]
  (let [conn-count (count @connections)]
    (max (int (/ @max-in-flight conn-count)) 1)))

(defn update-rdy [csmr {:keys [rdy last-rdy] :as conn} _]
  (let [current-rdy @rdy
        current-last-rdy @last-rdy
        per-conn-max (per-conn-max-in-flight csmr) ]
    (swap! rdy dec)
    (when (or (<= current-rdy 1)
              (<= current-rdy (/ current-last-rdy 4))
              (< per-conn-max current-rdy))
      (reset! rdy per-conn-max)
      (reset! last-rdy per-conn-max)
      (s/put! (get-in conn [:streams :sink]) (proto/encode :rdy per-conn-max)))))

(defn- substream [stream t]
  (s/filter #(= t (:type %)) stream))

(defn- split-stream [stream]
  (let [decoded-stream (proto/decode-stream stream)]
    {:sink (s/sink-only stream)
     :response (substream decoded-stream :response)
     :message (substream decoded-stream :message)
     :error (substream decoded-stream :error)}))

(defn producer->connection [{:strs [broadcast_address tcp_port]}]
  (let [client @(tcp/client {:host broadcast_address
                             :port tcp_port})]
    {:streams (split-stream client)
     :rdy (atom 1)
     :last-rdy (atom 1)
     :broadcast-address broadcast_address
     :port tcp_port}))

(defn err-handler [msg]
  (prn "ERROR" msg))

(defn response-handler [sink msg]
  (condp = (:body msg)
    "_heartbeat_"  (s/put! sink (proto/encode :nop))
    "OK" nil
    (prn "Unexpected message" msg)))

(defn on-closed [consumer connection]
  (println (str "lost connection to "
                (:broadcast-address connection) ":" (:port connection)))
  (swap! (:connections consumer) disj connection))

(defn subscribe-handlers [consumer]
  (doseq [conn @(:connections consumer)]
    (let [{:keys [sink error message response]} (:streams conn)]
      (s/on-closed sink (partial on-closed consumer conn))
      (s/consume (partial #'response-handler sink) response)
      (s/consume #'err-handler error)
      (s/consume (partial (:handler consumer) sink) message)
      (s/consume (partial #'update-rdy consumer conn) message))))

(defn lookup [topic lookupd-host]
  (let [lookupd-address (str lookupd-host "/lookup?topic=" topic)]
    (-> (d/timeout! (http/get lookupd-address) 200)
        (d/chain :body
                 bs/to-string
                 json/parse-string)
        (d/catch TimeoutException
          (fn [e] {:error e :address lookupd-address})))))

(defn print-errors [errors]
  (doseq [e errors]
    (println (format "error querying nsqlookupd (%s)"
                     (:address e) (:error e)))))

(defnk create [lookupd-http-address topic channel max-in-flight handler]
  (let [addresses (if (sequential? lookupd-http-address)
                    lookupd-http-address
                    (list lookupd-http-address))
        {errors true
         responses false} (->> addresses
                               (map (partial lookup topic))
                               (apply d/zip)
                               deref
                               (group-by #(contains? % :error)))
        producers (mapcat #(get-in % ["data" "producers"]) responses)
        connections (map producer->connection producers)
        consumer {:connections (atom (set connections))
                  :max-in-flight (atom max-in-flight)
                  :handler handler}]
    (print-errors errors)
    (subscribe-handlers consumer)
    (doseq [conn connections]
      (let [sink (get-in conn [:streams :sink])]
        (s/put! sink (proto/encode :magic-id))
        (s/put! sink (proto/encode :subscribe topic channel))
        (s/put! sink (proto/encode :rdy @(:rdy conn)))))
    consumer))

(defn finish! [sink id]
  (s/put! sink (proto/encode :fin id)))

(defn close! [consumer]
  (doseq [conn @(:connections consumer)
          :let [streams (:streams conn)]]
    (s/put! (:sink streams) (proto/encode :close))
    (dorun (map #(s/close! %) (vals streams)))))

(comment
  (defn handler [sink msg]
    (finish! sink (:id msg)))

  (def consumer (create {:lookupd-http-address ["http://localhost:4161"
                                                "http://localhost:5161"]
                         :topic "test"
                         :channel "test"
                         :max-in-flight 200
                         :handler #'handler}))
  )
