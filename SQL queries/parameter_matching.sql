SELECT 
t.TableTimeStamp as timetableStamp,
t.Parameter1 as TssTemperature, t.Parameter2 as MilkTemperature,
t.Parameter5 as AC_voltage, t.Parameter6 as CompressorCurrent,
t.Parameter7 as PumpCurrent, t.Parameter11 as TankSwitch,
t.Parameter15 as DischargePumpStatus, t.Parameter12 as AgitatorStatus
FROM aeron.Device_200011519698808 as t
