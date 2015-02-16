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

(defn update-rdy [consumer {:keys [rdy last-rdy] :as conn} _]
  (let [current-rdy @rdy
        current-last-rdy @last-rdy
        per-conn-max (per-conn-max-in-flight consumer) ]
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

(defn producer->connection [producer]
  (let [client @(tcp/client producer)]
    {:streams (split-stream client)
     :rdy (atom 1)
     :last-rdy (atom 1)}))

(defn err-handler [msg]
  (prn "ERROR" msg))

(defn response-handler [sink msg]
  (condp = (:body msg)
    "_heartbeat_"  (s/put! sink (proto/encode :nop))
    "OK" nil
    (prn "Unexpected message" msg)))

(defn on-closed [consumer producer]
  (println (str "lost connection to "
                (:host producer) ":" (:port producer)))
  (swap! (:connections consumer) dissoc producer))

(defn subscribe-handlers [consumer]
  (doseq [[producer conn] @(:connections consumer)]
    (let [{:keys [sink error message response]} (:streams conn)]
      (s/on-closed sink (partial on-closed consumer producer))
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

(defn responses->producers [responses]
  (->> (filter #(get % "data") responses)
       (mapcat #(get-in % ["data" "producers"]))
       (map (fn [{:strs [broadcast_address tcp_port]}]
              {:host broadcast_address
               :port tcp_port} ))))

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
        producers (responses->producers responses)
        connections (zipmap producers (map producer->connection producers))
        consumer {:connections (atom connections)
                  :max-in-flight (atom max-in-flight)
                  :handler handler}]
    (print-errors errors)
    (subscribe-handlers consumer)
    (doseq [conn (vals connections)]
      (let [sink (get-in conn [:streams :sink])]
        (s/put! sink (proto/encode :magic-id))
        (s/put! sink (proto/encode :subscribe topic channel))
        (s/put! sink (proto/encode :rdy @(:rdy conn)))))
    consumer))

(defn finish! [sink id]
  (s/put! sink (proto/encode :fin id)))

(defn close! [consumer]
  (doseq [conn (vals @(:connections consumer))
          :let [streams (:streams conn)]]
    (s/put! (:sink streams) (proto/encode :close))
    (dorun (map #(s/close! %) (vals streams)))))

(comment
  (defn handler [sink msg]
    (finish! sink (:id msg)))

  (defn make-consumer []
    (def consumer (create {:lookupd-http-address ["http://localhost:4161"
                                                  "http://localhost:5161"]
                           :topic "test"
                           :channel "test"
                           :max-in-flight 200
                           :handler #'handler})))
  )
