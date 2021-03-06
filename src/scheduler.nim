# scheduler
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
import sets

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
        tags: HashSet[string]
        nextRun:DateTime
        lastRun:DateTime
        startDay:WeekDay
        atTime:TimeObj
        interval:TimeInterval
        latest:TimeInterval
        scheduler:ptr Scheduler
        unit:unit
        # when T is void:
        jobFunc:proc()
        # else:
        #     jobFunc:proc(a:T)
        #     arg:T
    Scheduler* =ref object 
        jobs: seq[ref Job]
        # nextRun,lastRun:float
        
proc `<`(x:DateTime, y: TimeObj): bool =
    result = (x.hour,x.minute,x.second) < (y.hour,y.minute,y.second)

proc `<`*(self, other:Job):bool =
    ##[
    PeriodicJobs are sortable based on the scheduled time they
    run next.
    ]##
    return self.nextRun < other.nextRun

proc `$`*(self:TimeObj):string =
    result = [self.hour,self.minute,self.second].join(":")

proc hasValue(self:TimeObj):bool =
    result = (self.hour or self.minute or self.second) > 0

proc `$`*(self:Job):string = 
    let timestats = "(last run: $#, next run: $#)" % [$self.lastRun, $self.nextRun]

    # if hasattr(self.jobFunc, "__name__"):
    #     jobFunc_name = self.jobFunc.__name__
    # else:
    #     jobFunc_name = repr(self.jobFunc)
    # args = [repr(x) for x in self.jobFunc.args]
    # kwargs = ["%s=%s" % (k, repr(v))
    #             for k, v in self.jobFunc.keywords.items()]
    # call_repr = jobFunc_name + "(" + ", ".join(args + kwargs) + ")"
    let call_repr = repr(self.jobFunc)

    if (self.atTime.second or self.atTime.hour or self.atTime.minute) > 0:
        result = "Every $# at $# do $# $#" % [$self.interval, $self.atTime, call_repr, timestats]   
    else:
        result = "Every $# to $# do $# $#" % [$self.interval,$self.latest,call_repr,timestats]


proc tag*(self:var Job, tags:varargs[string]):Job =
    ##[
    Tags the job with one or more unique indentifiers.
    Tags must be hashable. Duplicate tags are discarded.
    :param tags: A unique list of ``Hashable`` tags.
    :return: The invoked job instance
    ]##
    self.tags.clear()
    for x in tags: self.tags.incl(x)
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
    assert self.unit in [udays,uhours] or self.startDay != invalid
    if self.unit == udays or self.startDay != invalid:
        hour = hour
    elif self.unit == uhours:
        hour = 0
    self.atTime = TimeObj(hour:hour, minute:minute)
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

converter toBool(a:TimeInterval):bool =
    let fields = [a.nanoseconds,a.microseconds,a.milliseconds,a.seconds,a.minutes,a.hours,a.days,a.months,a.years]
    let filled = filterIt(fields,it > 0)
    result = filled.len == 0

proc scheduleNextRun(self:ref Job) =
    ##[
    Compute the instant when this job should run next.
    ]##
    var interval:TimeInterval
    if not self.latest:
        assert self.latest - self.interval == false
        interval = ranInitInterval(self.latest)
    else:
        interval = self.interval
    # self.period = datetime.timedelta(**{self.unit: interval})

    self.nextRun = now() + interval
    if self.startDay != invalid :
        assert self.unit == uweeks
        var days_ahead = ord(self.startDay) - 1 - ord(self.nextRun.weekday)
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7
        self.nextRun += initTimeInterval(days=days_ahead) - interval
    if self.atTime.hasValue:
        assert self.unit in [udays, uhours] or self.startDay != invalid
        if self.unit == udays or self.startDay != invalid:
            self.nextRun.hour = self.atTime.hour
        self.nextRun.minute = self.atTime.minute
        self.nextRun.second = self.atTime.second
        # If we are running for the first time, make sure we run
        # at the specified time *today* (or *this hour*) as well
        if self.lastRun.monthday == 0:
            let now = now()
            if (self.unit == udays and self.atTime > now and
                    self.interval.days == 1):
                self.nextRun = self.nextRun - initTimeInterval(days = 1)
            elif self.unit == uhours and self.atTime.minute > now.minute:
                self.nextRun = self.nextRun - initTimeInterval(hours = 1)
    if self.startDay != invalid and self.atTime.hasValue:
        # Let"s see if we will still make that time we specified today
        if (self.nextRun - now()).days >= 7:
            self.nextRun -= interval
 
proc todo*(self:ref Job, jobFunc:proc()) :ref Job =
    ##[
    Specifies the jobFunc that should be called every time the
    job runs.
    Any additional arguments are passed on to jobFunc when
    the job runs.
    :param jobFunc: The function to be scheduled
    :return: The invoked job instance
    ]##
    self.jobFunc = jobFunc
    self.scheduleNextRun()
    self.scheduler.jobs.add(self)
    return self


proc shouldRun(self:ref Job):bool =
    ##[
    :return: ``True`` if the job should be run now.
    ]##
    return now() >= self.nextRun

proc run(self:ref Job) =
    ##[
    Run the job and immediately reschedule it.
    :return: The return value returned by the `jobFunc`
    ]##
    # logger.info("Running job %s", self)
    self.jobFunc()
    # let ret = self.jobFunc()
    self.lastRun = now()
    self.scheduleNextRun()
    # return ret

proc runJob(self:Scheduler, job:ref Job) =
    job.run()
    # if ret of CancelJob:
    #     self.cancel_job(job)
    
proc runPending*(self: Scheduler) =
    ##[
    Run all jobs that are scheduled to run.
    Please note that it is *intended behavior that run_pending()
    does not run missed jobs*. For example, if you've registered a job
    that should run every minute and you only call run_pending()
    in one hour increments then your job won't be run 60 times in
    between but only once.
    ]##
    var runnable_jobs = self.jobs.filter( proc(x:ref Job):bool = x.should_run ) 
    sort( runnable_jobs,proc (x, y: ref Job):int = cast[int]((x.nextRun - y.nextRun).seconds) )
    for job in runnable_jobs:
        self.runJob(job)

proc runAll*(self:Scheduler, delay_seconds=0) =
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

proc cancelJob*(self:var Scheduler, job:ref Job) = 
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
    doAssert filled.len == 1
    
proc every*(self:var Scheduler , interval:TimeInterval):ref Job {.discardable.}=
    ##[
    Schedule a new periodic job.
    :param interval: A quantity of a certain time unit
    :return: An unconfigured :class:`Job <Job>`
    ]##
    
    testInterval(interval)
    result = new Job
    result.interval = interval
    result.scheduler = addr(self)

proc getNextRun*(self: Scheduler):Option[DateTime] = 
    ##[
    Datetime when the next job should run.
    :return: A :class:`~datetime.datetime` object
    ]##
    if self.jobs.len != 0:
        result = some(min(self.jobs).nextRun)

proc every*(self:var Scheduler , interval:TimeInterval,body:proc())=
    ##[
    Schedule a new periodic job.
    :param interval: A quantity of a certain time unit
    :return: An unconfigured :class:`Job <Job>`
    ]##
    testInterval(interval)
    var result = new Job
    result.interval = interval
    result.scheduler = addr(self)
    result.jobFunc = body
    result.scheduleNextRun()
    result.scheduler.jobs.add(result)

proc idleSeconds*(self:Scheduler):int64 =
    ##[
    :return: Number of seconds until
                :meth:`nextRun <Scheduler.nextRun>`.
    ]##
    try:
        result = (self.getNextRun().get() - now()).seconds
    except UnpackError:
        result = -1

when isMainModule:
    proc job() =
        echo "I'm working...",now()
    var schedule = Scheduler()
    discard schedule.every(3.seconds).todo(job)
    schedule.every(3.seconds) do:
        echo "I'm working too..."
    # dumpTree:
    schedule.every 3.seconds:
        echo "I'm also working ..."
    while true:
        schedule.runPending()
        sleep(300)