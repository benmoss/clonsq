(ns clonsq.core-test
  (:require [clj-http.client :as http]
            [clojure.test :refer :all]
            [clonsq.core :as clonsq]))

(def http-port 14151)
(def tcp-port 14150)

(defn start-nsqd []
  (let [tempdir (java.nio.file.Files/createTempDirectory nil (into-array java.nio.file.attribute.FileAttribute []))
        opts [(str "-http-address=127.0.0.1:" http-port)
              (str "-tcp-address=127.0.0.1:" tcp-port)
              (str "-data-path=" (str tempdir))]
        proc (.exec (Runtime/getRuntime) (into-array (apply conj ["nsqd"] opts)))]
    (Thread/sleep 500)
    (try
      (when (> (.exitValue proc) 0)
        (throw (Exception. (slurp (.getErrorStream proc)))))
      (catch IllegalThreadStateException e))
    proc))

(defn stop-nsqd [nsqd]
  (.destroy nsqd)
  (Thread/sleep 500))

(defn publish [topic message]
  (http/post (str "http://127.0.0.1:" http-port "/pub")
               {:query-params {"topic" topic}
                :body message}))

(defn topic-fixture [f]
  (publish "test" "create_topic")
  (f)
  (publish "test" "delete_topic"))

(defn nsqd-fixture [f]
  (let [nsqd (start-nsqd)]
    (f)
    (stop-nsqd nsqd)))

(use-fixtures :once nsqd-fixture)
(use-fixtures :each topic-fixture)

(deftest a-test
  (testing "Reading a message"
    (let [result (promise)
          handler (fn [ch msg] (deliver result msg))
          _ (clonsq/subscribe {:topic "test" :channel "something" :handler handler :port tcp-port})
          _ (publish "test" "hi")]
      (is (= "hi" (deref result 10 nil))))))
