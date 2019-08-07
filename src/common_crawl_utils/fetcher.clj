(ns common-crawl-utils.fetcher
  (:require [clojure.tools.logging :as log]
            [common-crawl-utils.constants :as constants]
            [common-crawl-utils.coordinates :as coordinates]
            [org.httpkit.client :as http])
  (:import (java.io ByteArrayInputStream)
           (java.util Scanner)
           (java.util.zip GZIPInputStream)))

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
                (fn [{:keys [body error status]}]
                  (cond
                    (some? error) (if (pos-int? retry-count)
                                    (fetch-single-coordinate-content coordinate callback (dec retry-count) result-promise)
                                    (do
                                      (log/errorf "Error fetching content for coordinate `%s`: %s" coordinate error)
                                      (deliver result-promise (assoc coordinate :error (str error)))))
                    (not= status 206) (if (pos-int? retry-count)
                                        (fetch-single-coordinate-content coordinate callback (dec retry-count) result-promise)
                                        (do
                                          (log/errorf "Content fetch HTTP request for coordinate `%s` failed with status `%s`" coordinate status)
                                          (deliver result-promise (assoc coordinate :error (format "HTTP status `%s`: `%s`" status body)))))
                    :else (->> body (read-content) (assoc coordinate :content) (callback) (deliver result-promise)))))
  result-promise)

(defn fetch-content
  "Fetches coordinates from Common Crawl Index Server along with their content from AWS

  Takes `query` map, which is described in https://github.com/webrecorder/pywb/wiki/CDX-Server-API#api-reference
  and, optionally, `cdx-api` endpoint from https://index.commoncrawl.org/collinfo.json
  as well as `callback` function, which will be executed on fetched coordinate asynchronously

  ;; To fetch all content for host from most recent crawl
  (fetch-content {:url \"http://www.cnn.com\" :matchType \"host\"})

  ;; To fetch limited number of coordinates with content
  (take 10 (fetch-content {:url \"http://www.cnn.com\" :matchType \"host\"}))

  ;; Provide callback function
  (fetch-content {:url \"http://www.cnn.com\" :matchType \"host\"}
                 (:cdx-api (common-crawl-utils.coordinates/get-most-recent-crawl))
                 callback-function)"
  ([query]
   (fetch-content query (:cdx-api (coordinates/get-most-recent-crawl))))
  ([query cdx-api]
   (let [coordinates (coordinates/fetch query cdx-api)]
     (map #(if (:error %) % @(fetch-single-coordinate-content %)) coordinates)))
  ([query cdx-api callback]
   (future
     (doseq [coordinate-portion (->> (coordinates/fetch query cdx-api) (remove :error) (partition-all 1000))]
       (run! deref (doall (map #(fetch-single-coordinate-content % :callback callback) coordinate-portion)))))))
