# schedule
# Copyright zhoupeng
# job scheduling for humans
import times
import sequtils
import os
import options
import strscans
import algorithm

type
    unit = enum
        milliseconds,seconds,minutes,hours,days,months,years
    Job*[T]= object 
        tags: seq[string]
        next_run:DateTime
        last_run:float
        start_day:WeekDay
        at_time:DateTime
        scheduler:Scheduler
        when T is void:
            job_func:proc()
        else:
            job_func:proc(a:T)
            arg:T
    Scheduler* = object 
        jobs: seq[Job[typed]]
        next_run,last_run:float
        
    
proc initJob(self:Job, interval:TimeInterval, scheduler:Scheduler):Job =
    discard
    # self.interval = interval  # pause interval * unit between runs
    # self.latest = None  # upper limit to the interval
    # self.job_func = None  # the job job_func to run
    # self.unit = None  # time units, e.g. "minutes", "hours", ...
    # self.at_time = None  # optional time at which this job runs
    # self.last_run = None  # datetime of the last run
    # self.next_run = None  # datetime of the next run
    # self.period = None  # timedelta between runs, only valid for
    # self.start_day = None  # Specific day of the week to start on
    # self.tags = set()  # unique set of tags for the job
    # self.scheduler = scheduler  # scheduler to register with

proc `<`*(self, other:Job):bool =
    ##[
    PeriodicJobs are sortable based on the scheduled time they
    run next.
    ]##
    return self.next_run < other.next_run

proc `$`(self:Job):string = 
    discard
    # proc format_time(t):
    #     return t.strftime("%Y-%m-%d %H:%M:%S") if t else "[never]"

    # timestats = "(last run: %s, next run: %s)" % (
    #             format_time(self.last_run), format_time(self.next_run))

    # if hasattr(self.job_func, "__name__"):
    #     job_func_name = self.job_func.__name__
    # else:
    #     job_func_name = repr(self.job_func)
    # args = [repr(x) for x in self.job_func.args]
    # kwargs = ["%s=%s" % (k, repr(v))
    #             for k, v in self.job_func.keywords.items()]
    # call_repr = job_func_name + "(" + ", ".join(args + kwargs) + ")"

    # if self.at_time is not None:
    #     return "Every %s %s at %s do %s %s" % (
    #             self.interval,
    #             self.unit[:-1] if self.interval == 1 else self.unit,
    #             self.at_time, call_repr, timestats)
    # else:
    #     fmt = (
    #         "Every %(interval)s " +
    #         ("to %(latest)s " if self.latest is not None else "") +
    #         "%(unit)s do %(call_repr)s %(timestats)s"
    #     )

    #     return fmt % dict(
    #         interval=self.interval,
    #         latest=self.latest,
    #         unit=(self.unit[:-1] if self.interval == 1 else self.unit),
    #         call_repr=call_repr,
    #         timestats=timestats
    #     )


proc tag(self:var Job, tags:varargs[string]):Job =
    ##[
    Tags the job with one or more unique indentifiers.
    Tags must be hashable. Duplicate tags are discarded.
    :param tags: A unique list of ``Hashable`` tags.
    :return: The invoked job instance
    ]##
    self.tags.setLen 0
    for x in tags: self.tags.add(x)
    return self

proc at(self:Job, time_str:string):Job =
    ##[
    Schedule the job every day at a specific time.
    Calling this is only valid for jobs scheduled to run
    every N day(s).
    :param time_str: A string in `XX:YY` format.
    :return: The invoked job instance
    ]##
    var 
        hour:HourRange
        minute:MinuteRange
    
    if scanf(time_str, "$i:$i", hour,minute):
        self.at_time = DateTime(hour:hour, minute:minute)
    elif scanf(time_str, "$i", minute):
        self.at_time = DateTime(hour:hour, minute:minute)
    # assert self.unit in ("days", "hours") or self.start_day
    # hour, minute = time_str.split(":")
    # minute = int(minute)
    if self.unit == days or self.start_day:
        hour = hour
        assert 0 <= hour <= 23
    elif self.unit == hours:
        hour = 0
    # assert 0 <= minute <= 59
    return self

# proc to(self:Job, latest):Job =
#     ##[
#     Schedule the job to run at an irregular (randomized) interval.
#     The job"s interval will randomly vary from the value given
#     to  `every` to `latest`. The range defined is inclusive on
#     both ends. For example, `every(A).to(B).seconds` executes
#     the job function every N seconds such that A <= N <= B.
#     :param latest: Maximum interval between randomized job runs
#     :return: The invoked job instance
#     ]##
#     self.latest = latest
#     return self

proc todo[T](self:Job[T], job_func:proc(a:T), arg:T) : Job[T] =
    ##[
    Specifies the job_func that should be called every time the
    job runs.
    Any additional arguments are passed on to job_func when
    the job runs.
    :param job_func: The function to be scheduled
    :return: The invoked job instance
    ]##
    # self.job_func = functools.partial(job_func, *args, **kwargs)
    # try:
    #     functools.update_wrapper(self.job_func, job_func)
    # except AttributeError:
    #     # job_funcs already wrapped by functools.partial won"t have
    #     # __name__, __module__ or __doc__ and the update_wrapper()
    #     # call will fail.
    #     pass
    self.schedule_next_run()
    self.scheduler.jobs.append(self)
    return self


proc should_run(self:Job):bool =
    ##[
    :return: ``True`` if the job should be run now.
    ]##
    return now() >= self.next_run

proc run(self:Job) :auto =
    ##[
    Run the job and immediately reschedule it.
    :return: The return value returned by the `job_func`
    ]##
    # logger.info("Running job %s", self)
    let ret = self.job_func()
    self.last_run = epochTime()
    self.schedule_next_run()
    return ret

proc schedule_next_run(self:Job) =
    """
    Compute the instant when this job should run next.
    """
    # assert self.unit in ("seconds", "minutes", "hours", "days", "weeks")
    var interval:TimeInterval
    if self.latest isnot nil:
        assert self.latest >= self.interval
        # interval = random.randint(self.interval, self.latest)
    else:
        interval = self.interval

    # self.period = datetime.timedelta(**{self.unit: interval})
    self.next_run = epochTime() + interval
    if self.start_day isnot nil:
        assert self.unit == weeks
        # weekdays = (
        #     "monday",
        #     "tuesday",
        #     "wednesday",
        #     "thursday",
        #     "friday",
        #     "saturday",
        #     "sunday"
        # )
        # assert self.start_day in weekdays
        # weekday = weekdays.index(self.start_day)
        let days_ahead = ord(self.start_day) - self.next_run.weekday()
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7
        self.next_run += initDuration(days:days_ahead) - interval
    if self.at_time isnot nil:
        assert self.unit in (days, hours) or self.start_day isnot nil
        # kwargs = {
        #     "minute": self.at_time.minute,
        #     "second": self.at_time.second,
        #     "microsecond": 0
        # }
        if self.unit == days or self.start_day isnot nil:
            self.next_run.hour = self.at_time.hour
        # self.next_run = self.next_run.replace(**kwargs)
        self.next_run.minute = self.at_time.minute
        self.next_run.second = self.at_time.second
        # If we are running for the first time, make sure we run
        # at the specified time *today* (or *this hour*) as well
        if not self.last_run:
            let now = initDateTime()
            if (self.unit == days and self.at_time > now.time() and
                    self.interval == 1):
                self.next_run = self.next_run - initDuration(days:1)
            elif self.unit == hours and self.at_time.minute > now.minute:
                self.next_run = self.next_run - initDuration(hours:1)
    if self.start_day isnot nil and self.at_time isnot nil:
        # Let"s see if we will still make that time we specified today
        if (self.next_run - epochTime()).days >= 7:
            self.next_run -= self.period
            
proc initScheduler*(self:Scheduler):Scheduler =
    # self.jobs = @[]
    discard

proc run_pending*[T](self:Scheduler) =
    ##[
    Run all jobs that are scheduled to run.
    Please note that it is *intended behavior that run_pending()
    does not run missed jobs*. For example, if you've registered a job
    that should run every minute and you only call run_pending()
    in one hour increments then your job won't be run 60 times in
    between but only once.
    ]##
    let runnable_jobs = self.jobs.filter( proc(x:Job[T]):bool = x.should_run ) 
    sort(runnable_jobs, system.cmp[int])
    for job in runnable_jobs:
        self.runJob(job)

proc run_all[T](self:Scheduler, delay_seconds=0) =
    ##[
    Run all jobs regardless if they are scheduled to run or not.
    A delay of `delay` seconds is added between each job. This helps
    distribute system load generated by the jobs more evenly
    over time.
    :param delay_seconds: A delay added between every executed job
    ]##
    # logger.info("Running *all* %i jobs with %is delay inbetween",
    #             len(self.jobs), delay_seconds)
    for job in self.jobs:
        self.runJob(job)
        sleep(delay_seconds*1000)

proc clear[T](self:var Scheduler, tag:string) = 
    ##[
    Deletes scheduled jobs marked with the given tag, or all jobs
    if tag is omitted.
    :param tag: An identifier used to identify a subset of
                jobs to delete
    ]##
    if tag.len == 0:
        self.jobs.setLen(0)
    else:
        self.jobs = self.jobs.filter( proc(x:Job[T]):bool = tag notin x.tags )

proc cancel_job(self:var Scheduler, job:Job) = 
    ##[
    Delete a scheduled job.
    :param job: The job to be unscheduled
    ]##
    try:
        self.jobs.del(job)
    except ValueError:
        discard

proc every(self:var Scheduler, interval:TimeInterval):Job=
    ##[
    Schedule a new periodic job.
    :param interval: A quantity of a certain time unit
    :return: An unconfigured :class:`Job <Job>`
    ]##
    result = Job(interval:interval, scheduler:self)

proc runJob(self:Scheduler, job:Job) =
    let ret = job.run()
    # if ret of CancelJob:
    #     self.cancel_job(job)

proc get_next_run(self: Scheduler):Option[DateTime] = 
    ##[
    Datetime when the next job should run.
    :return: A :class:`~datetime.datetime` object
    ]##
    if self.jobs.len != 0:
        result = some(min(self.jobs).next_run)


proc idle_seconds(self:Scheduler):Natural =
    ##[
    :return: Number of seconds until
                :meth:`next_run <Scheduler.next_run>`.
    ]##
    return (self.next_run - epochTime()).toInt()
    
when isMainModule:
    proc job() =
        echo "I'm working..."
    var schedule = Scheduler()
    schedule.every(10.minutes).todo(job)