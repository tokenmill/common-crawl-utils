(ns common-crawl-utils.fetcher
  (:require [common-crawl-utils.constants :as constants]
            [common-crawl-utils.coordinates :as coordinates]
            [common-crawl-utils.utils :as utils]
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

(defn fetch-single-coordinate-content [coordinate]
  @(http/request {:url     (str constants/cc-s3-base-url (get coordinate :filename))
                  :method  :get
                  :headers {"range" (get-range-header coordinate)}
                  :as      :byte-array
                  :timeout constants/http-timeout}
                 (fn [{:keys [body error status] :as response}]
                   (if (or (some? error) (not= status 206))
                     (assoc coordinate :error (utils/get-http-error response))
                     (-> coordinate (dissoc :error) (assoc :content (read-content body)))))))

(defn fetch-content
  "Fetches coordinates from Common Crawl Index Server along with their content from AWS

  Takes `query` map, described in https://github.com/webrecorder/pywb/wiki/CDX-Server-API#api-reference

  Additionally, `:cdx-api` query key can specify index server endpoint.
  If `:cdx-api` is not provided, endpoint from most recent crawl is used and
  can be accesed with `(common-crawl-utils.config/get-most-recent-cdx-api)`

  ;; To fetch all content for host from most recent crawl
  (fetch-content {:url \"http://www.cnn.com\" :matchType \"host\"})

  ;; To fetch limited number of coordinates with content
  (take 10 (fetch-content {:url \"http://www.cnn.com\" :matchType \"host\"}))"
  [query]
  (map (fn [{error :error :as coordinate}]
         (cond->> coordinate (nil? error) (fetch-single-coordinate-content)))
       (coordinates/fetch query)))
