(ns nsq-tail
  (:require [clonsq.core :as nsq]))

(def messages-shown (atom 0))

(defn handler [total-messages conn msg]
  (swap! messages-shown inc)
  (prn msg)
  (nsq/finish msg conn)
  (when (and (> total-messages 0)
             (>= @messages-shown total-messages))
    (System/exit 0)))

(defn run [{:keys [lookupd-http-address topic channel max-in-flight total-messages]}]
  (nsq/connect {:lookupd-http-address lookupd-http-address
                :topic topic
                :channel channel
                :max-in-flight max-in-flight
                :handler (partial handler total-messages)}))

(defn normalize-args [args]
  (zipmap (map #(keyword (clojure.string/replace % #"--" ""))
               (keys args))
          (vals args)))

(defn -main [& args]
  (let [defaults {"--max-in-flight" 200
                  "--total-messages" 0}
        args (->> (apply hash-map args)
                  (merge defaults)
                  normalize-args)]
    (when (not (:topic args))
      (println "--topic is required")
      (System/exit 1))
    (when (not (:lookupd-http-address args))
      (println "--lookupd-http-address is required")
      (System/exit 1))
    (run args)))

(comment
  (def conn (-main "--lookupd-http-address" "http://localhost:4161" "--topic" "test" "--channel" "test"))
  (manifold.stream/put! @conn "CLS\n")
  (manifold.stream/close! @conn)

  )
