(ns common-crawl-utils.utils
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [warc-clojure.core :as warc])
  (:import (java.io PushbackReader InputStreamReader BufferedReader)
           (java.util.zip GZIPInputStream)
           (org.jwat.warc WarcReaderFactory)))

(defn read-edn [path]
  (-> path
      (io/reader)
      (PushbackReader.)
      (edn/read)))

(defn gzip-line-seq [path]
  (-> path
      (io/input-stream)
      (GZIPInputStream.)
      (InputStreamReader.)
      (BufferedReader.)
      (line-seq)))

(defn warc-record-seq [path]
  (-> path
      (io/input-stream)
      (WarcReaderFactory/getReader)
      (warc/get-response-records-seq)))

(defn request-json [url]
  (http/request {:url url :method :get}
                (fn [{:keys [status body]}]
                  (when (= status 200)
                    (json/decode body true)))))
