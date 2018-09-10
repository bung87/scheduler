# schedule
# Copyright zhoupeng
# job scheduling for humans
import times
import sequtils
import os
import options
import strscans
import algorithm
import strutils
import random
import macros

{.experimental.}

type
    TimeObj = object
        second: SecondRange  
        hour:HourRange
        minute: MinuteRange 
    WeekDay* = enum ## Represents a weekday.
        invalid,dMon, dTue, dWed, dThu, dFri, dSat, dSun
    unit = enum
        umilliseconds,useconds,uminutes,uhours,udays,uweeks,umonths,uyears
    Job* = object 
        tags: seq[string]
        next_run:DateTime
        last_run:DateTime
        start_day:WeekDay
        at_time:TimeObj
        interval:TimeInterval
        latest:TimeInterval
        scheduler:ptr Scheduler
        unit:unit
        # when T is void:
        job_func:proc()
        # else:
        #     job_func:proc(a:T)
        #     arg:T
    Scheduler* = object 
        jobs: seq[ref Job]
        # next_run,last_run:float
        
proc `<`(x:DateTime, y: TimeObj): bool =
    result = (x.hour,x.minute,x.second) < (y.hour,y.minute,y.second)

proc `<`*(self, other:Job):bool =
    ##[
    PeriodicJobs are sortable based on the scheduled time they
    run next.
    ]##
    return self.next_run < other.next_run

proc `$`*(self:TimeObj):string =
    result = [self.hour,self.minute,self.second].join(":")

proc hasValue(self:TimeObj):bool =
    result = (self.hour or self.minute or self.second) > 0

proc `$`*(self:Job):string = 
    let timestats = "(last run: $#, next run: $#)" % [$self.last_run, $self.next_run]

    # if hasattr(self.job_func, "__name__"):
    #     job_func_name = self.job_func.__name__
    # else:
    #     job_func_name = repr(self.job_func)
    # args = [repr(x) for x in self.job_func.args]
    # kwargs = ["%s=%s" % (k, repr(v))
    #             for k, v in self.job_func.keywords.items()]
    # call_repr = job_func_name + "(" + ", ".join(args + kwargs) + ")"
    let call_repr = repr(self.job_func)

    if (self.at_time.second or self.at_time.hour or self.at_time.minute) > 0:
        result = "Every $# at $# do $# $#" % [$self.interval, $self.at_time, call_repr, timestats]   
    else:
        result = "Every $# to $# do $# $#" % [$self.interval,$self.latest,call_repr,timestats]


proc tag*(self:var Job, tags:varargs[string]):Job =
    ##[
    Tags the job with one or more unique indentifiers.
    Tags must be hashable. Duplicate tags are discarded.
    :param tags: A unique list of ``Hashable`` tags.
    :return: The invoked job instance
    ]##
    self.tags.setLen 0
    for x in tags: self.tags.add(x)
    return self

proc at*(self:var Job, time_str:string):Job =
    ##[
    Schedule the job every day at a specific time.
    Calling this is only valid for jobs scheduled to run
    every N day(s).
    :param time_str: A string in `XX:YY` format.
    :return: The invoked job instance
    ]##
    var 
        hour:int
        minute:int
    
    if scanf( time_str, "$i:$i" , hour ,minute ):
        discard
    elif scanf(time_str, "$i", minute):
        discard
    assert self.unit in [udays,uhours] or self.start_day != invalid
    if self.unit == udays or self.start_day != invalid:
        hour = hour
    elif self.unit == uhours:
        hour = 0
    self.at_time = TimeObj(hour:hour, minute:minute)
    # assert 0 <= minute <= 59
    return self

proc to*(self:var Job, latest:TimeInterval):Job =
    ##[
    Schedule the job to run at an irregular (randomized) interval.
    The job"s interval will randomly vary from the value given
    to  `every` to `latest`. The range defined is inclusive on
    both ends. For example, `every(A).to(B).seconds` executes
    the job function every N seconds such that A <= N <= B.
    :param latest: Maximum interval between randomized job runs
    :return: The invoked job instance
    ]##
    self.latest = latest
    return self

proc ranInitInterval(a:TimeInterval):TimeInterval =
    let ms = rand(a.milliseconds)
    let sec = rand(a.seconds)
    let m = rand(a.minutes)
    let hours = rand(a.hours)
    let days = rand(a.days)
    let months = rand(a.months)
    let years = rand(a.years)
    result = initTimeInterval(ms,sec,m,hours,days,months,years)

proc empty(a:TimeInterval):bool =
    let fields = [a.nanoseconds,a.microseconds,a.milliseconds,a.seconds,a.minutes,a.hours,a.days,a.months,a.years]
    let filled = filterIt(fields,it > 0)
    result = filled.len == 0

proc schedule_next_run(self:ref Job) =
    ##[
    Compute the instant when this job should run next.
    ]##
    var interval:TimeInterval
    if not self.latest.empty():
        assert (self.latest - self.interval).empty() == false
        interval = ranInitInterval(self.latest)
    else:
        interval = self.interval
    # self.period = datetime.timedelta(**{self.unit: interval})

    self.next_run = now() + interval
    if self.start_day != invalid :
        assert self.unit == uweeks
        var days_ahead = ord(self.start_day) - 1 - ord(self.next_run.weekday)
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7
        self.next_run += initTimeInterval(days=days_ahead) - interval
    if self.at_time.hasValue:
        assert self.unit in [udays, uhours] or self.start_day != invalid
        if self.unit == udays or self.start_day != invalid:
            self.next_run.hour = self.at_time.hour
        self.next_run.minute = self.at_time.minute
        self.next_run.second = self.at_time.second
        # If we are running for the first time, make sure we run
        # at the specified time *today* (or *this hour*) as well
        if self.last_run.monthday == 0:
            let now = now()
            if (self.unit == udays and self.at_time > now and
                    self.interval.days == 1):
                self.next_run = self.next_run - initTimeInterval(days = 1)
            elif self.unit == uhours and self.at_time.minute > now.minute:
                self.next_run = self.next_run - initTimeInterval(hours = 1)
    if self.start_day != invalid and self.at_time.hasValue:
        # Let"s see if we will still make that time we specified today
        if (self.next_run - now()).days >= 7:
            self.next_run -= interval
 
proc todo*(self:ref Job, job_func:proc()) :ref Job =
    ##[
    Specifies the job_func that should be called every time the
    job runs.
    Any additional arguments are passed on to job_func when
    the job runs.
    :param job_func: The function to be scheduled
    :return: The invoked job instance
    ]##
    self.job_func = job_func
    self.schedule_next_run()
    self.scheduler.jobs.add(self)
    return self


proc should_run(self:ref Job):bool =
    ##[
    :return: ``True`` if the job should be run now.
    ]##
    return now() >= self.next_run

proc run(self:ref Job) =
    ##[
    Run the job and immediately reschedule it.
    :return: The return value returned by the `job_func`
    ]##
    # logger.info("Running job %s", self)
    self.job_func()
    # let ret = self.job_func()
    self.last_run = now()
    self.schedule_next_run()
    # return ret

proc runJob(self:Scheduler, job:ref Job) =
    job.run()
    # if ret of CancelJob:
    #     self.cancel_job(job)
    
proc run_pending*(self: Scheduler) =
    ##[
    Run all jobs that are scheduled to run.
    Please note that it is *intended behavior that run_pending()
    does not run missed jobs*. For example, if you've registered a job
    that should run every minute and you only call run_pending()
    in one hour increments then your job won't be run 60 times in
    between but only once.
    ]##
    var runnable_jobs = self.jobs.filter( proc(x:ref Job):bool = x.should_run ) 
    sort( runnable_jobs,proc (x, y: ref Job):int = cast[int]((x.next_run - y.next_run).seconds) )
    for job in runnable_jobs:
        self.runJob(job)

proc run_all*(self:Scheduler, delay_seconds=0) =
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

proc clear*(self:var Scheduler, tag:string) = 
    ##[
    Deletes scheduled jobs marked with the given tag, or all jobs
    if tag is omitted.
    :param tag: An identifier used to identify a subset of
                jobs to delete
    ]##
    if tag.len == 0:
        self.jobs.setLen(0)
    else:
        self.jobs = self.jobs.filter( proc(x:ref Job):bool = tag notin x.tags )

proc cancel_job(self:var Scheduler, job:ref Job) = 
    ##[
    Delete a scheduled job.
    :param job: The job to be unscheduled
    ]##
    try:
        let index = self.jobs.find(job)
        self.jobs.del(index)
    except ValueError:
        discard

proc testInterval(a:TimeInterval) =
    let fields = [a.milliseconds,a.seconds,a.minutes,a.hours,a.days,a.months,a.years]
    let filled = filterIt(fields,it > 0)
    assert filled.len == 1
    
proc every*(self:var Scheduler , interval:TimeInterval):ref Job {.discardable.}=
    ##[
    Schedule a new periodic job.
    :param interval: A quantity of a certain time unit
    :return: An unconfigured :class:`Job <Job>`
    ]##
    testInterval(interval)
    result = new(Job)
    result.interval = interval
    result.scheduler = addr(self)

proc get_next_run*(self: Scheduler):Option[DateTime] = 
    ##[
    Datetime when the next job should run.
    :return: A :class:`~datetime.datetime` object
    ]##
    if self.jobs.len != 0:
        result = some(min(self.jobs).next_run)

proc every*(self:var Scheduler , interval:TimeInterval,body:proc())=
    ##[
    Schedule a new periodic job.
    :param interval: A quantity of a certain time unit
    :return: An unconfigured :class:`Job <Job>`
    ]##
    testInterval(interval)
    var result = new(Job)
    result.interval = interval
    result.scheduler = addr(self)
    result.job_func = body
    result.schedule_next_run()
    result.scheduler.jobs.add(result)

proc idle_seconds*(self:Scheduler):Natural =
    ##[
    :return: Number of seconds until
                :meth:`next_run <Scheduler.next_run>`.
    ]##
    return (self.get_next_run().get() - now()).seconds

when isMainModule:
    proc job() =
        echo "I'm working..."
    var schedule = Scheduler()
    discard schedule.every(3.seconds).todo(job)
    schedule.every(3.seconds) do:
        echo "I'm working too..."
    # dumpTree:
    schedule.every 3.seconds:
            echo "I'm also working ..."
    while true:
        schedule.run_pending()
        sleep(300)