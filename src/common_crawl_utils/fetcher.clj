(ns common-crawl-utils.fetcher
  (:require [clojure.string :as str]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [common-crawl-utils.constants :as constants]
            [clojure.tools.logging :as log])
  (:import (java.util.zip GZIPInputStream)
           (java.io ByteArrayInputStream)
           (java.util Scanner)))

(defn- parse-query-response [body]
  (if (= -1 (.indexOf body "\"error\": \"No Captures found for"))
    (->> body
         (str/split-lines)
         (map #(json/parse-string % true)))
    []))

(defn submit-query [query cdx-api & {:keys [retry-count result-promise] :or {retry-count 0 result-promise (promise)}}]
  (http/request {:url          cdx-api
                 :method       :get
                 :query-params (assoc query :output "json" :pageSize 1)}
                (fn [{:keys [body error]}]
                  (try
                    (if error
                      (throw error)
                      (deliver result-promise (parse-query-response body)))
                    (catch Exception e
                      (if (> 3 retry-count)
                        (submit-query query cdx-api :retry-count (inc retry-count) :result-promise result-promise)
                        (do
                          (deliver result-promise [])
                          (log/errorf "Error submitting query %s to '%s': %s" query cdx-api (Throwable->map e))))))))
  @result-promise)

(defn get-number-of-pages [query cdx-api]
  (-> query
      (assoc :showNumPages true)
      (submit-query cdx-api)
      (first)
      (get :pages)))

(defn fetch-coordinates [query cdx-api]
  (->> (get-number-of-pages query cdx-api)
       (range)
       (mapcat #(submit-query (assoc query :page %) cdx-api))))

(defn- get-range-header [{:keys [offset length]}]
  (let [offset (Integer/parseInt offset)
        length (Integer/parseInt length)]
    (format "bytes=%s-%s" offset (dec (+ offset length)))))

(defn- read-content [body]
  (with-open [rdr (-> body (ByteArrayInputStream.) (GZIPInputStream.) (Scanner.))]
    {:warc   (.next (.useDelimiter rdr "\r\n\r\n"))
     :header (.next (.useDelimiter rdr "\r\n\r\n"))
     :html   (.next (.useDelimiter rdr "\\A"))}))

(defn fetch-single-coordinate-content [coordinate & {:keys [callback retry-count result-promise] :or {callback identity retry-count 0 result-promise (promise)}}]
  (http/request {:url     (str constants/cc-s3-base-url (get coordinate :filename))
                 :method  :get
                 :headers {"range" (get-range-header coordinate)}
                 :as      :byte-array
                 :timeout 5000}
                (fn [{:keys [body error]}]
                  (try
                    (if error
                      (throw error)
                      (deliver result-promise (callback (assoc coordinate :content (read-content body)))))
                    (catch Exception e
                      (if (> 3 retry-count)
                        (fetch-single-coordinate-content coordinate :callback callback :retry-count (inc retry-count) :result-promise result-promise)
                        (do
                          (deliver result-promise (assoc coordinate :error e))
                          (log/errorf "Error fetching coordinate '%s' content: %s" coordinate (Throwable->map e))))))))
  result-promise)

(defn fetch-content [query & {:keys [cdx-api callback portion-size] :or {cdx-api (get constants/most-recent-crawl :cdx-api) portion-size 1000}}]
  (let [coordinates (fetch-coordinates query cdx-api)]
    (if callback
      (future
        (doseq [coordinate-portion (partition-all portion-size coordinates)]
          (run! deref (doall (map #(fetch-single-coordinate-content % :callback callback) coordinate-portion)))))
      (map (comp deref fetch-single-coordinate-content) coordinates))))
