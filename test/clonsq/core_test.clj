(ns clonsq.core-test
  (:require [clojure.test :refer :all]))

(deftest a-test
  (testing "FIXME, I fail."
    (is (= 0 1))))

(defn start-nsqd []
  (let [tempdir (java.nio.file.Files/createTempDirectory nil (into-array java.nio.file.attribute.FileAttribute []))
        opts ["-http-address=127.0.0.1:14151"
              "-tcp-address=127.0.0.1:14150"
              (str "-data-path=" (str tempdir))]
        proc (.exec (Runtime/getRuntime) (into-array (apply conj ["nsqd"] opts)))]
    (Thread/sleep 500)
    (try
      (when (> (.exitValue proc) 0)
        (throw (Exception. (slurp (.getErrorStream proc)))))
      (catch IllegalThreadStateException e))
    proc))

(defn stop-nsqd [nsqd]
  (.destroy nsqd))
