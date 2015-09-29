(ns bonney.core)

(defrecord SchedulePool [executor jobs description id-atom])
(defrecord RecurringJob [token id created-at interval delay task description])
(defrecord FixedDelayRecurringJob [token id created-at interval delay task description])
(defrecord ScheduledJob [token id created-at delay pool task description])

(def TIME_UNIT java.util.concurrent.TimeUnit/MILLISECONDS)

(defn now [] (System/currentTimeMillis))

(defn- add-job
  "This is like the bedroom, it's where the magic happens. The job gets
   scheduled with the pool's executor and a record of the job is added to
   the pool."
  [pool job-token f interval delay interspaced? description]
  (let [job-map { :id          (swap! (:id-atom pool) inc) 
                  :token       job-token
                  :created-at  (System/currentTimeMillis) 
                  :delay       delay
                  :description description }
        scheduled? (> 1 interval)
        job (cond 
              scheduled? (-> (map->ScheduledJob job-map)
                             (assoc :task (.schedule (:executor pool)
                                                    f
                                                    delay
                                                    TIME_UNIT)))
              interspaced? (-> (map->FixedDelayRecurringJob job-map)
                                (assoc :interval interval)
                                (assoc :task (.scheduleWithFixedDelay (:executor pool)
                                                                     f
                                                                     delay
                                                                     interval
                                                                     TIME_UNIT)))
              :else (-> (map->RecurringJob job-map)
                        (assoc :interval interval)
                        (assoc :task (.scheduleAtFixedRate (:executor pool)
                                                          f
                                                          delay
                                                          interval
                                                          TIME_UNIT))))]
    (assoc-in pool [:jobs job-token] job)))

(defn- pool-alive?
  "Return whether the pool is alive."
  [pool]
  (-> pool :executor .isShutdown not))

(defn- remove-job
  "Remove the identified job from the pool."
  [pool job-token]
  (update-in pool [:jobs] dissoc job-token))

(defn- stop-job
  "Stops/cancels the identified job and removes it from the pool."
  [pool job-token kill]
  (let [job (get-in pool [:jobs job-token])]
    (.cancel (:task job) kill)
    (remove-job pool job-token)))

(defn- stop-all-jobs
  "Stops all jobs managed by a pool."
  [pool aggressive]
  (let [job-token (-> pool :jobs keys first)]
    (if-not job-token
      pool
      (recur (stop-job pool job-token aggressive) aggressive))))

(defn- shutdown-pool
  "Agent handler for shutting down a pool by cancelling all pending jobs
   and shutting down the executor."
  [pool aggressive]
  (let [executor (:executor pool)
        pool (stop-all-jobs pool aggressive)]
    (when (pool-alive? pool)
        (.shutdown executor))
    pool))

(defn- get-job-by-id
  "Returns jobs by their ID in a pool."
  [pool id]
  (->> @pool 
       :jobs 
       vals 
       (filter #(= id (:id %))) 
       first))

(defn create-pool
  "Creates a new scheduler pool. All jobs scheduled within this pool will
   share an executor and be constrained by the number of threads in that
   executor. Default worker thread count is 1."
  [& {:keys [threads desc] :or {threads 1 desc""}}]
  (agent (->SchedulePool (java.util.concurrent.Executors/newScheduledThreadPool threads)
                         {}
                         desc
                         (atom 0))))

(defn- create-token [pool] (Object.))

(defn- wrap-cleaner-fn
  "Wraps a scheduled job to remove it from the pool after it runs."
  [pool job-token f]
  (fn []
    (f)
    (send pool remove-job job-token)))

(defn- wrap-error-fn
  "Wraps a job to catch any Exceptions that occur and propogate them
   to a handler function instead of silently failing."
  [error-f f]
  (fn []
    (try
      (f)
      (catch InterruptedException e nil)
      (catch Exception e (error-f e)))))

(defn every
  "Calls the specified function every interval milliseconds. An :error-fn can be
  specified to receive any exceptions for handling. The function returns an
  object that can be used to cancel the job.
  
  Default options are :initial-delay, :desc, and :error-fn"
  [interval f pool & {:keys [error-fn initial-delay desc] :or {initial-delay 0 desc""}}]
  (let [job-token (create-token pool)
        final-fn (wrap-error-fn (or error-fn identity) f)]
    (send pool add-job job-token final-fn interval initial-delay false desc)
    { :pool pool :token job-token }))
          
(defn interspaced
  "Calls the specified function with interval milliseconds between the end of the
  call and the beginning of the next. An :error-fn can be
  specified to receive any exceptions for handling. The function returns an
  object that can be used to cancel the job.
  
  Default options are :initial-delay, :desc, and :error-fn"
  [interval f pool & {:keys [error-fn initial-delay desc] :or {initial-delay 0 desc""}}]
  (let [job-token (create-token pool)
        final-fn (wrap-error-fn (or error-fn identity) f)]
    (send pool add-job job-token final-fn interval initial-delay true desc)
    { :pool pool :token job-token }))

(defn at
  "Calls the specified function one time at the specified time (in milliseconds).
  An :error-fn can be specified to receive any exceptions for handling. 
  The function returns an object that can be used to cancel the job.
  
  Default options are :desc, and :error-fn"
  [time f pool & {:keys [error-fn desc] :or {desc""}}]
  (let [job-token (create-token pool)
        delay (- time (System/currentTimeMillis))
        final-fn (->> f 
                      (wrap-error-fn (or error-fn identity))
                      (wrap-cleaner-fn pool job-token))]
    (send pool add-job job-token final-fn 0 delay false desc)
    { :pool pool :token job-token }))

(defn after
  "Calls the specified function one time after the specified delay (in milliseconds).
  An :error-fn can be specified to receive any exceptions for handling. 
  The function returns an object that can be used to cancel the job.
  
  Default options are :desc, and :error-fn"
  [delay f pool & {:keys [error-fn desc] :or {desc""}}]
  (let [job-token (create-token pool)
        final-fn (->> f 
                      (wrap-error-fn (or error-fn identity))
                      (wrap-cleaner-fn pool job-token))]
    (send pool add-job job-token final-fn 0 delay false desc)
    { :pool pool :token job-token }))

(defn stop
  "Stops a job peacefully."
  ([job]
    (send (:pool job) stop-job (:token job) false))
  ([id pool]
    (when-let [token (:token (get-job-by-id pool id))]
      (send pool stop-job token false))))

(defn kill
  "Stops a job aggressively by interrupting it if it's running."
  ([job]
    (send (:pool job) stop-job (:token job) true))
  ([id pool]
    (when-let [token (:token (get-job-by-id pool id))]
      (send pool stop-job token true))))

(defn shutdown
  "Shuts down a pool which will cancel/terminate all jobs and
   terminate the background executor threads."
  ([pool] (shutdown pool false))
  ([pool aggressive]
     (send pool shutdown-pool aggressive)))

(defn scheduled-jobs
  "Returns all jobs in a given pool."
  [pool]
  (-> (deref pool)
      :jobs
      vals))
