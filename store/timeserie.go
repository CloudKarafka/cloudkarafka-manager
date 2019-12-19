package store

type TimeSerie interface {
	Interval() int
	All() []int
	Last() int
	Len() int
}

type SimpleTimeSerie struct {
	interval int
	Points   []int `json:"points"`
	latest   int
}

func NewSimpleTimeSerie(interval, maxPoints int) *SimpleTimeSerie {
	return &SimpleTimeSerie{
		interval: interval,
		Points:   make([]int, maxPoints),
		latest:   -1,
	}
}

func (me *SimpleTimeSerie) Add(y int) {
	if me.latest == -1 {
		me.latest = y
		return
	}
	v := (y - me.latest) / me.interval
	me.latest = y
	copy(me.Points, me.Points[1:])
	me.Points[me.Len()-1] = v
}
func (me *SimpleTimeSerie) Interval() int {
	return me.interval
}
func (me *SimpleTimeSerie) All() []int {
	return me.Points[:me.Len()]
}
func (me *SimpleTimeSerie) Last() int {
	if len(me.Points) == 0 {
		return 0
	} else {
		return me.Points[me.Len()-1]
	}
}
func (me *SimpleTimeSerie) Len() int {
	return len(me.Points)
}

type SumTimeSerie struct {
	Series []TimeSerie
}

func NewSumTimeSerie(series []TimeSerie) TimeSerie {
	return &SumTimeSerie{Series: series}
}
func (me *SumTimeSerie) Interval() int {
	return me.Series[0].Interval()
}
func (me *SumTimeSerie) All() []int {
	res := make([]int, me.Len())
	for _, serie := range me.Series {
		for i, p := range serie.All() {
			res[i] += p
		}
	}
	return res
}

func (me *SumTimeSerie) Last() int {
	all := me.All()
	l := len(all)
	if l == 0 {
		return 0
	}
	return all[len(all)-1]
}

func (me *SumTimeSerie) Len() int {
	l := 0
	for _, serie := range me.Series {
		if serie.Len() > l {
			l = serie.Len()
		}
	}
	return l
}
