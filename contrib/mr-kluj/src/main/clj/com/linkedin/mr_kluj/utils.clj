;Copyright 2010 LinkedIn, Inc
;
;Licensed under the Apache License, Version 2.0 (the "License"); you may not
;use this file except in compliance with the License. You may obtain a copy of
;the License at
;
;http://www.apache.org/licenses/LICENSE-2.0
;
;Unless required by applicable law or agreed to in writing, software
;distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
;WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
;License for the specific language governing permissions and limitations under
;the License.

(ns com.linkedin.mr-kluj.utils)

(defn make-select-all-fn
  [fields]
  "Creates a (fn [map] ...) that selects all keys as in select-keys from the map.

   fields is a seq of either srings or [name default] vectors.
     a string value will simply select the field if it exists and insert nil if it does not exist.
     a vector value specifies the name of the field as well as the default value."
  (let [selection-fns
        (map
          (fn [field-spec]
            (if (string? field-spec)
              (fn [in-map] [field-spec (get in-map field-spec nil)])
              (let [[field-name default-value] field-spec]
                (fn [in-map] [field-name (get in-map field-name default-value)]))))
          fields)]
    (fn [in-map] (into {} (map #(% in-map) selection-fns)))))