import sys
from ExternalFunctions import ProcessFitbit
morphdata=ProcessFitbit()

morphdata.build('steps')

morphdata.readsummarized('steps')
#read in steps
#ID, Date, steps, std dev steps,


#option exists to aggregate over 'week' 'month', default is DAY and avg
#day requires nothing
#week requires summing steps over week, intervention week can be added later

#morphdata.aggregate('week_calendar')

morphdata.applyfilter()

morphdata.analysis()

morphdata.steppatterning()
morphdata.hourpatterning()

morphdata.combothesis()


