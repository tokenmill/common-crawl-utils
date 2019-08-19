(ns common-crawl-utils.coordinates-test
  (:require [clojure.test :refer :all]
            [common-crawl-utils.coordinates :as coordinates]))

(deftest ^:integration coordinates-test
  (let [query-1 {:url     "tokenmill.lt"
                 :filter  ["digest:U3FWVBI7XZ2KVBD72MRR7TCHHXSX2FJS"]
                 :cdx-api "http://index.commoncrawl.org/CC-MAIN-2019-09-index"}
        query-2 {:url       "delfi.lt"
                 :matchType "host"
                 :cdx-api   "http://index.commoncrawl.org/CC-MAIN-2019-09-index"}
        coordinate-1 {:offset        "272838009",
                      :digest        "U3FWVBI7XZ2KVBD72MRR7TCHHXSX2FJS",
                      :mime          "text/html",
                      :charset       "UTF-8",
                      :mime-detected "text/html",
                      :filename      "crawl-data/CC-MAIN-2019-09/segments/1550247481992.39/warc/CC-MAIN-20190217111746-20190217133746-00381.warc.gz",
                      :status        "200",
                      :urlkey        "lt,tokenmill)/",
                      :url           "http://tokenmill.lt/",
                      :length        "8548",
                      :languages     "eng",
                      :timestamp     "20190217125141"}]
    (testing "Query submission"
      (is (= [coordinate-1] (coordinates/call-cdx-api query-1))))
    (testing "Page counting"
      (is (= 1 (:pages (coordinates/get-total-pages
                         {:url     "tokenmill.lt/*"
                          :cdx-api "http://index.commoncrawl.org/CC-MAIN-2019-09-index"}))))
      (is (= 0 (:pages (coordinates/get-total-pages
                         {:url     "-"
                          :cdx-api "http://index.commoncrawl.org/CC-MAIN-2019-09-index"})))))
    (testing "Coordinate fetching"
      (is (= [coordinate-1] (coordinates/fetch query-1))))
    (testing "Fething single coordinate for multi page result"
      (is (some? (first (coordinates/fetch query-2)))))))
