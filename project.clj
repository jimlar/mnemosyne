(defproject mnemosynedb "0.1.0-SNAPSHOT"
  :description "The database that never forgets"
  :url "https://github.com/jimlar/mnemosyne"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.0"]
                 [aleph "0.3.0-beta14"]]
  :main mnemosyne.server
  :profiles {
    :dev {
      :dependencies [[midje "1.5.0"]]
      :plugins [[lein-midje "3.0.0"]]
    }
})
