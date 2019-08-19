(ns common-crawl-utils.coordinates
  (:require [clojure.core.async :refer [>!! chan close! thread]]
            [clojure.tools.logging :as log]
            [common-crawl-utils.constants :as constants]
            [common-crawl-utils.utils :as utils]
            [org.httpkit.client :as http]))

(def cdx-params [:url :from :to :matchType :limit :sort :filter
                 :fl :page :pageSize :showNumPages :showPagedIndex])

(defn get-most-recent-cdx-api
  [{:keys [index-collinfo]
    :as   query
    :or   {index-collinfo constants/index-collinfo}}]
  (let [{:keys [cdx-api error]} (utils/get-most-recent-crawl index-collinfo)]
    (cond-> query
            (some? cdx-api) (assoc :cdx-api cdx-api)
            (some? error) (assoc :error error))))

(defn call-cdx-api [{:keys [cdx-api timeout] :as query}]
  (log/debugf "Calling `%s` with query `%s`" cdx-api (select-keys query cdx-params))
  @(http/request {:url          cdx-api
                  :method       :get
                  :timeout      (or timeout constants/http-timeout)
                  :query-params (-> query (select-keys cdx-params) (assoc :output "json"))}
                 (fn [{:keys [body error status] :as response}]
                   (cond
                     (= status 404) []
                     (or (some? error)
                         (not= status 200)) (-> query (assoc :error (utils/get-http-error response)) (vector))
                     :else (utils/read-jsonl body)))))

(defn get-total-pages [{:keys [cdx-api] :as query}]
  (let [[{:keys [pages error]}] (when cdx-api (-> query (assoc :showNumPages true) (call-cdx-api)))]
    (cond-> query
            (some? pages) (assoc :pages pages)
            (some? error) (assoc :error error))))

(defn- validate-query [{:keys [cdx-api error page pages showNumPages] :as query}]
  (cond-> query
          (some? error) (dissoc :error)
          (some? showNumPages) (dissoc :showNumPages)
          (nil? cdx-api) (get-most-recent-cdx-api)
          (nil? pages) (get-total-pages)
          (nil? page) (assoc :page 0)))

(defn fetch
  "Issues HTTP request to Common Crawl Index Server and returns a lazy sequence with content coordinates

  Takes `query` map, described in https://github.com/webrecorder/pywb/wiki/CDX-Server-API#api-reference

  Additionally, `:cdx-api` query key can specify index server endpoint.
  If `:cdx-api` is not provided, endpoint from most recent crawl is used and
  can be accesed with `(common-crawl-utils.config/get-most-recent-cdx-api)`

  ;; To fetch all coordinates for host from most recent crawl
  (fetch {:url \"http://www.cnn.com\" :matchType \"host\"})

  ;; To fetch limited number of coordinates
  (take 10 (fetch {:url \"http://www.cnn.com\" :matchType \"host\"}))"
  [query]
  (lazy-seq
    (let [{:keys [page pages error] :as validated-query} (validate-query query)]
      (cond
        (zero? pages) (vector)
        (some? error) (vector validated-query)
        (< page pages) (concat (call-cdx-api validated-query)
                               (fetch (update validated-query :page inc)))))))

(defn fetch-async
  [{:keys [coordinate-chan limit close?]
    :as   query
    :or   {coordinate-chan (chan)
           close?          true}}]
  (thread
    (doseq [coordinate (cond->> (fetch query) (some? limit) (take limit))]
      (>!! coordinate-chan coordinate))
    (when close?
      (close! coordinate-chan)
      (log/debugf "Closed coordinate channel for query `%s`" (select-keys query cdx-params))))
  coordinate-chan)
