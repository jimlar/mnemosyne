(defproject mnemosynedb "0.1.0-SNAPSHOT"
  :description "The database that never forgets"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]]
  :repositories {"stuart" "http://stuartsierra.com/maven2"}
;  :main storage.main
  :profiles {
    :dev {
      :plugins [[lein-midje "2.0.3"]]
    }
    :test {
      :dependencies [
        [midje "1.4.0"]
        [com.stuartsierra/lazytest "1.2.3"]
      ]
    }
  })
