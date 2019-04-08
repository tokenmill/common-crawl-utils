(ns common-crawl-utils.constants-test
  (:require [clojure.test :refer :all]
            [common-crawl-utils.constants :as constants]))

(deftest constants-test
  (testing "Constants not nil"
    (are [constant] (some? constant)
                    constants/config
                    constants/cc-s3-base-url
                    constants/crawls
                    constants/most-recent-crawl)))
