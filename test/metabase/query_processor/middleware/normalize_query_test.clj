(ns metabase.query-processor.middleware.normalize-query-test
  (:require [metabase.query-processor.middleware.normalize-query :as normalize]
            [expectations :refer :all]))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                NORMALIZE TOKENS                                                |
;;; +----------------------------------------------------------------------------------------------------------------+


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  CANONICALIZE                                                  |
;;; +----------------------------------------------------------------------------------------------------------------+

;;; ---------------------------------------------- canonicalize-clauses ----------------------------------------------

(expect
  [:field-id 10]
  (#'normalize/wrap-implicit-field-id 10))

(expect
  {:query {:aggregation [[:count [:field-id 10]]]}}
  (#'normalize/canonicalize-clauses {:query {:aggregation [:count 10]}}))

(expect
  {:query {:aggregation [[:count]]}}
  (#'normalize/canonicalize-clauses {:query {:aggregation [:count]}}))

(expect
  {:query {:aggregation [[:count [:field-id 1000]]]}}
  (#'normalize/canonicalize-clauses {:query {:aggregation [:count [:field-id 1000]]}}))

;; :rows aggregation type, being deprecated FOREVER, should just get removed
(expect
  {:query {:aggregation nil}}
  (#'normalize/canonicalize-clauses {:query {:aggregation [:rows]}}))

(expect
  {:query {:aggregation nil}}
  (#'normalize/canonicalize-clauses {:query {:aggregation :rows}}))

;; if just a single aggregation is supplied it should always be converted to new-style multiple-aggregation syntax
(expect
  {:query {:aggregation [[:count]]}}
  (#'normalize/canonicalize-clauses {:query {:aggregation :count}}))

;; Raw Field IDs in an aggregation should get wrapped in [:field-id] clause
(expect
  {:query {:aggregation [[:count [:field-id 10]]]}}
  (#'normalize/canonicalize-clauses {:query {:aggregation [:count 10]}}))

;; implicit Field IDs should get wrapped in [:field-id] in :breakout
(expect
  {:query {:breakout [[:field-id 10]]}}
  (#'normalize/canonicalize-clauses {:query {:breakout [10]}}))

(expect
  {:query {:breakout [[:field-id 10] [:field-id 20]]}}
  (#'normalize/canonicalize-clauses {:query {:breakout [10 20]}}))

(expect
  {:query {:breakout [[:field-id 1000]]}}
  (#'normalize/canonicalize-clauses {:query {:breakout [[:field-id 1000]]}}))

(expect
  {:query {:fields [[:field-id 10]]}}
  (#'normalize/canonicalize-clauses {:query {:fields [10]}}))

;; implicit Field IDs should get wrapped in [:field-id] in :fields
(expect
  {:query {:fields [[:field-id 10] [:field-id 20]]}}
  (#'normalize/canonicalize-clauses {:query {:fields [10 20]}}))

(expect
  {:query {:fields [[:field-id 1000]]}}
  (#'normalize/canonicalize-clauses {:query {:fields [[:field-id 1000]]}}))

;; implicit Field IDs should get wrapped in [:field-id] in filters
(expect
  {:query {:filter [:= [:field-id 10] 20]}}
  (#'normalize/canonicalize-clauses {:query {:filter [:= 10 20]}}))

(expect
  {:query {:filter [:and [:= [:field-id 10] 20] [:= [:field-id 20] 30]]}}
  (#'normalize/canonicalize-clauses {:query {:filter [:and
                                                      [:= 10 20]
                                                      [:= 20 30]]}}))

(expect
  {:query {:filter [:between [:field-id 10] 20 30]}}
  (#'normalize/canonicalize-clauses {:query {:filter [:between 10 20 30]}}))

;; compound filters with only one arg should get automatically de-compounded
(expect
  {:query {:filter [:= [:field-id 100]]}}
  (#'normalize/canonicalize-clauses {:query {:filter [:and [:= 100]]}}))

(expect
  {:query {:filter [:= [:field-id 100]]}}
  (#'normalize/canonicalize-clauses {:query {:filter [:or [:= 100]]}}))


;;; ---------------------------------------------- remove-empty-clauses ----------------------------------------------

;; empty sequences should get removed
(expect
  {:y [100]}
  (#'normalize/remove-empty-clauses {:x [], :y [100]}))

;; nil values should get removed
(expect
  {:y 100}
  (#'normalize/remove-empty-clauses {:x nil, :y 100}))

;; sequences containing only nil should get removed
(expect
  {:a [nil 100]}
  (#'normalize/remove-empty-clauses {:a [nil 100], :b [nil nil]}))

;; empty maps should get removed
(expect
  {:a {:b 100}}
  (#'normalize/remove-empty-clauses {:a {:b 100}, :c {}}))

(expect
  {:a {:b 100}}
  (#'normalize/remove-empty-clauses {:a {:b 100}, :c {:d nil}}))
