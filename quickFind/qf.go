package quickFind

//QF holds the IDs of components
type QF struct {
	ID []int
}

//InitializeQF initializes the DS by putting each element in its own compoenent
func InitializeQF(n int) QF {
	q := QF{make([]int, n)}

	for i := 0; i < n; i++ {
		q.ID[i] = i
	}
	return q
}

//Find returns the ID of component for which the vertex belongs to
func (q *QF) Find(p int) int { return q.ID[p] }

//Connected checks if the elements have the same parent
func (q *QF) Connected(e1, e2 int) bool {

	return q.Find(e1) == q.Find(e2)
}

//Union merges the components of the two elements.
func (q *QF) Union(s, t int) {
	sid := q.ID[s]
	tid := q.ID[t]
	for i := 0; i < len(q.ID); i++ {
		if q.ID[i] == sid {
			q.ID[i] = tid
		}
	}
}

//GetComponents returns the total number of components
func (q *QF) GetComponents() map[int]int {
	componentTrees := make(map[int]int)
	for _, c := range q.ID {
		componentTrees[c]++
	}
	return componentTrees
}
