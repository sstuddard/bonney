# Bonney

William H. Bonney, a Lincoln County *Regulator*

Bonney is a simple scheduler for Clojure that closely mimics the [overtone/at-at](https://github.com/overtone/at-at) interface. The only significant difference is shutdown. 

There are two primary differences. Bonney aims to cleanly and completely shutdown when asked, and to provide error handling support. With Java executors, a task throwing an exception fails silently and makes the executor unusable.

## General

A pool maps to an individual executor and group of jobs. When creating a pool you may specify the number of workers, the default is 1. Jobs are created per pool and killed per pool. You should always close all pools when exiting your program.

There are two ways to stop jobs, aggressively and not aggressively. If a job is stopped, it will complete running if it is already running. If a job is killed, it will be interrupted if it is running already.

## Installation and Usage

From Clojars, put in your project.clj:
```clojure
[bonney "0.1."]
```

```clojure
;; In your ns statement:
(:require [bonney.core :refer :all])
```

## Examples

```clojure
;; create a pool
(def pool (create-pool :threads 4 :desc "My new pool"))

;; start a one time job 5 seconds from now
(at (+ (now) 5000) #(println "Just once!") pool)

;; or start a one time job after 5 seconds
(after 5000 #(println "I'm from the future") pool)

;; start a job with error handling
(after 5000 #(throw (Exception. "error!")) pool :error-fn #(println "there was an error"))

;; start a recurring job that runs every 5 seconds
(def job (every 5000 #(println "Time to do work") pool))

;; stop the job
(kill job)

;; Shutdown the pool
(shutdown pool)
```

## License

Copyright © 2015 Shayne Studdard

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
