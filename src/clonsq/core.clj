(ns clonsq.core
  (:import (java.net Socket)))

(defn connect [host port]
  (let [socket (doto (Socket. host port)
                 (.setTcpNoDelay true)
                 (.setKeepAlive true)
                 (.setSoTimeout (or timeout-ms 0)))
        in-stream (-> (.getInputStream socket)
                      (BufferedInputStream.)
                      (DataInputStream.))
        out-stream (-> (.getOutputStream socket)
                       (PrintWriter.))]))

(def socket (connect "localhost" 4150))

(defn subscribe [{:keys [topic channel handler host port]}]
  (let [host (or host "127.0.0.1")
        port (or port 4150)
        connection (connect host port)]
    ))

