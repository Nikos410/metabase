(ns metabase.query-processor.middleware.normalize-query
  "Middleware that normalizes the keys in query map into standard `lisp-case` keywords; useful because MBQL queries are
  case-insensitive, string/keyword insensitive, and underscore/hyphen insensitive."
  (:require [clojure.walk :as walk]
            [metabase.query-processor.util :as qputil]))

(defn- normalize [x]
  (cond
    (record? x)
    x

    (map? x)
    (into {} (for [[k v] x]
               [(qputil/normalize-token k) (normalize v)]))

    (sequential? x)
    (map normalize x)

    ((some-fn keyword? string?) x)
    (qputil/normalize-token x)

    :else
    x))

(defn normalize-query
  [query]
  (-> (normalize query)
      (update :type qputil/normalize-token)))

(defn normalize-query-middleware
  "Middleware that normalizes a query and converts all keys to standardized `lisp-case` keywords, so we don't need to
  worry about legacy support for `SNAKE_CASE` and the like in other places."
  [qp]
  (comp qp normalize-query))
