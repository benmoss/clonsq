(ns clonsq.core
  (:require [aleph.http :as http]
            [byte-streams :as bs]
            [cheshire.core :as json]
            [clonsq.consumer :as c]
            [clonsq.protocol :as proto]
            [manifold.deferred :as d]
            [manifold.stream :as s]))


(defn lookup [lookupd-host topic]
  (d/chain (http/get (str lookupd-host "/lookup?topic=" topic))
           :body
           bs/to-string
           json/parse-string))

(defn connect [{:keys [lookupd-http-address topic] :as opts}]
  (let [addresses (if (sequential? lookupd-http-address)
                    lookupd-http-address
                    (list lookupd-http-address))
        responses @(apply d/zip (map #(lookup % topic) addresses))
        producers (mapcat #(get-in % ["data" "producers"]) responses)
        consumer (c/create producers opts)]
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

  (def consumer (connect {:lookupd-http-address ["http://localhost:4161"
                                                 "http://localhost:5161"]
                          :topic "test"
                          :channel "test"
                          :max-in-flight 200
                          :handler #'handler}))
  )
