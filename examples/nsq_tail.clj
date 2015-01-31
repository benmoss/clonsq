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
  (let [args (apply hash-map args)
        required-args ["--topic" "--lookupd-http-address"]
        default-args {"--max-in-flight" 200
                      "--total-messages" 0}
        normalized (->> args
                        (merge default-args)
                        normalize-args)]
    (when-let [missing-args (seq (remove (partial contains? args) required-args))]
      (doseq [arg-name missing-args]
        (println (str arg-name " is required")))
      (System/exit 1))
    (run normalized)))
