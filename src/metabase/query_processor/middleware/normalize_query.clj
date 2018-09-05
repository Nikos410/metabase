(ns metabase.query-processor.middleware.normalize-query
  "Middleware that normalizes the keys in query map into standard `lisp-case` keywords; useful because MBQL queries are
  case-insensitive, string/keyword insensitive, and underscore/hyphen insensitive."
  (:require [metabase.query-processor.util :as qputil]
            [metabase.util :as u]
            [clojure.walk :as walk]
            [medley.core :as m]))

;; Normalization is broken out into two steps:
;;
;; *  Normalize tokens: This step converts tokens into standard `lisp-case` keywords. Things like "STARTS_WITH" and
;;    fIElD_ID, which are legal MBQL, get converted to `:starts-with` and `:field-id`, respectively.
;;
;; *  Canonicalize: Converts the query into a canonical form by doing a series of transformations such as removing
;;    the long-deprecated `rows` aggregation type, adding `field-id` clauses where implicit, and removing empty clauses.

(defn- mbql-clause? [x]
  (and (sequential? x)
       ((some-fn keyword? string?) (first x))))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                NORMALIZE TOKENS                                                |
;;; +----------------------------------------------------------------------------------------------------------------+

(declare normalize-tokens)

(def ^:private path->special-token-normalization-fn
  "Map of special functions that should be used to perform token normalization for a given path. For example, the
  `:expressions` key in an MBQL query should preserve the case of the expression names; this custom behavior is
  defined below."
  {:type  qputil/normalize-token
   :query {:aggregation
           ;; for old-style aggregations like {:aggregation :count} make sure we normalize the ag type
           (fn [ag-clause]
             (if ((some-fn keyword? string?) ag-clause)
               (qputil/normalize-token ag-clause)
               (normalize-tokens ag-clause)))

           :expressions
           ;; for expressions, we don't want to normalize the name of the expression; keep that as is.
           (partial m/map-vals normalize-tokens)}})

(defn- normalize-tokens
  "Recursively normalize a query and return the canonical form of that query."
  [x & [path]]
  (let [special-fn (when (seq path)
                     (get-in path->special-token-normalization-fn path))]
    (cond
      (fn? special-fn)
      (special-fn x)

      (record? x)
      x

      (map? x)
      (into {} (for [[k v] x
                     :let  [k (qputil/normalize-token k)]]
                 [k (normalize v (conj (vec path) k))]))

      ;; MBQL clause
      (mbql-clause? x)
      (vec (cons (qputil/normalize-token (first x))
                 (map normalize (rest x))))

      (sequential? x)
      (mapv normalize x)

      :else
      x)))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  CANONICALIZE                                                  |
;;; +----------------------------------------------------------------------------------------------------------------+

;;; ---------------------------------------------- canonicalize-clauses ----------------------------------------------

(defn- wrap-implicit-field-id [field]
  (if (integer? field)
    [:field-id field]
    field))

(defn- canonicalize-aggregation-subclause [[ag-type field, :as ag-subclause]]
  (cond
    (= ag-type :rows)
    nil

    field
    [ag-type (wrap-implicit-field-id field)]

    :else
    ag-subclause))

(defn- canonicalize-aggregations [aggregations]
  (mapv canonicalize-aggregation-subclause
        ;; replace old-style single-ags like {:aggregation :count} with new-style {:aggregation [:count]}
        (cond
          ;; something like {:aggregations :count}
          (keyword? aggregations)
          [[aggregations]]

          (mbql-clause? aggregations)
          [aggregations]

          :else
          aggregations)))

(defn- canonicalize-filter [[filter-name & args, :as filter-subclause]]
  (cond
    ;; for `and` or `not` compound filters with only one subclase, just unnest the subclause
    (and (#{:and :or} filter-name)
         (= (count args) 1))
    (canonicalize-filter (first args))

    ;; for other `and`/`or`/`not` compound filters, recurse on the arg(s)
    (#{:and :or :not} filter-name)
    (vec (cons filter-name (map canonicalize-filter args)))

    ;; all the other filter types have an implict field ID for the first arg
    ;; (e.g. [:= 10 20] gets canonicalized to [:= [:field-id 10] 20]
    (#{:= :!= :< :<= :> :>= :is-null :not-null :between :inside :starts-with :ends-with :contains :does-not-contain
       :time-interval} filter-name)
    (apply vector filter-name (wrap-implicit-field-id (first args)) (rest args))))

(defn- canonicalize-order-by [order-by-clause])

(defn- canonicalize-mbql-clauses [inner-query]
  (cond-> inner-query
    (:aggregation inner-query) (update :aggregation canonicalize-aggregations)
    (:breakout    inner-query) (update :breakout    (partial mapv wrap-implicit-field-id))
    (:fields      inner-query) (update :fields      (partial mapv wrap-implicit-field-id))
    (:filter      inner-query) (update :filter      canonicalize-filter)))

(defn- canonicalize-clauses
  "Canonicalize a query [MBQL query], rewriting the query as if you perfectly followed the recommended style guides for
  writing MBQL. Does things like removes unneeded and empty clauses, converts older MBQL '95 syntax to MBQL '98, etc."
  [outer-query]
  (cond-> outer-query
    (:query outer-query) (update :query canonicalize-mbql-clauses)))


;;; ---------------------------------------------- remove-empty-clauses ----------------------------------------------


(defn- non-empty-value?
  "Is this 'value' in a query map considered non-empty (e.g., should we refrain from removing that key entirely?) e.g.:

    {:aggregation nil} ; -> remove this, value is nil
    {:filter []}       ; -> remove this, also empty
    {:limit 100}       ; -> keep this"
  [x]
  (cond
    ;; a map is considered non-empty if it has some keys
    (map? x)
    (seq x)

    ;; a sequence is considered non-empty if it has some non-nil values
    (sequential? x)
    (and (seq x)
         (some some? x))

    ;; any other value is considered non-empty if it is not nil
    :else
    (some? x)))

(defn- remove-empty-clauses [query]
  (walk/postwalk
   (fn [x]
     (cond
       (record? x)
       x

       (map? x)
       (m/filter-vals non-empty-value? x)

       :else
       x))
   query))


;;; ---------------------------------------------- Putting it together -----------------------------------------------

(def ^{:arglists '([query])} ^:private canonicalize
  (comp remove-empty-clauses canonicalize-clauses))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                            PUTTING IT ALL TOGETHER                                             |
;;; +----------------------------------------------------------------------------------------------------------------+

(def ^{:arglists '([query])} normalize
  (comp canonicalize normalize-tokens))

(defn normalize-query-middleware
  "Middleware that normalizes a query and converts all keys to standardized `lisp-case` keywords, so we don't need to
  worry about legacy support for `SNAKE_CASE` and the like in other places."
  [qp]
  (comp qp normalize))
