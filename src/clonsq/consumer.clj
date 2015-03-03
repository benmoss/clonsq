(ns clonsq.consumer
  (:require [aleph.http :as http]
            [aleph.tcp :as tcp]
            [byte-streams :as bs]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [clonsq.protocol :as proto]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [plumbing.core :refer [defnk letk]]))

(defnk per-conn-max-in-flight [max-in-flight connections]
  (let [conn-count (count @connections)]
    (max (int (/ @max-in-flight (max conn-count 1))) 1)))

(defn update-rdy [consumer {:keys [rdy last-rdy] :as conn} _]
  (let [current-rdy @rdy
        current-last-rdy @last-rdy
        per-conn-max (per-conn-max-in-flight consumer)]
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

(defn create-connection! [producer]
  (let [client @(tcp/client producer)]
    {:streams (split-stream client)
     :rdy (atom 1)
     :client client
     :last-rdy (atom 1)}))

(defn err-handler [msg]
  (log/fatal "ERROR" msg))

(defn response-handler [sink msg]
  (condp = (:body msg)
    "_heartbeat_"  (s/put! sink (proto/encode :nop))
    "OK" nil
    (log/fatal "Unexpected message" msg)))

(defn remove-connection! [consumer producer]
  (log/debugf "lost connection to %s:%s" (:host producer) (:port producer))
  (swap! (:connections consumer) dissoc producer))

(defn print-errors [errors]
  (doseq [e errors]
    (log/errorf "error querying nsqlookupd (%s)" (:address e))))

(defn responses->producers [responses]
  (->> (filter #(get % "data") responses)
       (mapcat #(get-in % ["data" "producers"]))
       (map (fn [{:strs [broadcast_address tcp_port]}]
              {:host broadcast_address
               :port tcp_port} ))))

(defn perform-handshake! [connection {:keys [topic channel]}]
  (let [sink (get-in connection [:streams :sink])]
    (s/put! sink (proto/encode :magic-id))
    (s/put! sink (proto/encode :subscribe topic channel))
    (s/put! sink (proto/encode :rdy @(:rdy connection)))))

(defn subscribe-handlers! [producer connection consumer]
  (letk [[sink error message response] (:streams connection)]
    (s/on-closed sink (partial remove-connection! consumer producer))
    (s/consume (partial #'response-handler sink) response)
    (s/consume #'err-handler error)
    (s/consume (partial (:handler consumer) sink) message)
    (s/consume (partial #'update-rdy consumer connection) message)))

(defn add-connections! [{:keys [connections] :as consumer} {:keys [errors responses]}]
  (print-errors errors)
  (doseq [producer (responses->producers responses)
          :when (not (get @connections producer))]
    (let [connection (create-connection! producer)]
      (log/debugf "connected to %s:%s" (:host producer) (:port producer))
      (swap! connections assoc producer connection)
      (perform-handshake! connection consumer)
      (subscribe-handlers! producer connection consumer))))

(defn lookup! [topic lookupd-host]
  (let [lookupd-address (str lookupd-host "/lookup?topic=" topic)]
    (-> (d/timeout! (http/get lookupd-address) 1000)
        (d/chain :body
                 bs/to-string
                 json/parse-string)
        (d/catch (fn [e] {:exception e :address lookupd-host})))))

(defn query-lookups! [lookupd-addresses topic]
  (->> lookupd-addresses
       (map (partial lookup! topic))
       (apply d/zip)
       deref
       (group-by #(if (contains? % :exception)
                    :errors
                    :responses))))

(defnk create [lookupd-http-address topic channel
               max-in-flight handler lookupd-poll-interval]
  (let [addresses (if (sequential? lookupd-http-address)
                    lookupd-http-address
                    (list lookupd-http-address))
        query-fn (partial query-lookups! addresses topic)
        polling-stream (s/periodically lookupd-poll-interval query-fn)
        consumer {:connections (atom {})
                  :max-in-flight (atom max-in-flight)
                  :handler handler
                  :lookupd-addresses addresses
                  :topic topic
                  :channel channel
                  :polling-stream polling-stream
                  }]
    (add-connections! consumer (query-fn))
    (s/consume (partial #'add-connections! consumer) polling-stream)
    consumer))

(defn finish! [sink id]
  (s/put! sink (proto/encode :fin id)))

(defn close! [consumer]
  (s/close! (:polling-stream consumer))
  (doseq [conn (vals @(:connections consumer))]
    (s/put! (get-in conn [:streams :sink]) (proto/encode :close))
    (s/close! (:client conn))))

; (comment

  ; (defn make-consumer []
    ; (defn handler [sink msg]
      ; (prn (bs/to-string (:body msg)))
      ; (finish! sink (:id msg)))
    ; (def consumer (create {:lookupd-http-address ["http://0.0.0.0:4161"]
                           ; :topic "test"
                           ; :channel "test"
                           ; :max-in-flight 200
                           ; :handler #'handler})))
  ; )
