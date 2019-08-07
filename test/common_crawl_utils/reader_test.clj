(ns common-crawl-utils.reader-test
  (:require [clojure.test :refer :all]
            [common-crawl-utils.reader :as reader]))

(deftest ^:integration reader-test
  (testing "Warc reading"
    (is (some some? (reader/read-warc))))
  (testing "Coordinate reading"
    (is (some some? (reader/read-coordinates)))))
