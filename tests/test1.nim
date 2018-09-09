import times
import sequtils

proc testInterval(a:TimeInterval) =
    let fields = [a.milliseconds,a.seconds,a.minutes,a.hours,a.days,a.months,a.years]
    let filled = filterIt(fields,it > 0)
    assert filled.len == 1

testInterval(3.hours)

var a =new(TimeInterval)
echo a.seconds
