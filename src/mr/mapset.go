package mr

type MapSet struct {
	mapBool map[interface{}] bool  // 指示这个map任务是否处理完毕
	count int
}

func NewMapSet() *MapSet {
	m := MapSet{}
	m.mapBool = make(map[interface{}]bool)
	m.count = 0
	return &m;
}

func (m *MapSet) Find(fileId int) bool {
	if m.mapBool[fileId] {
		return true
	}

	return false
}

func (m *MapSet) Size() int {
	return m.count
}

func (m *MapSet) Insert(fileId int) {
	m.mapBool[fileId] = true
	m.count++
}

func (m *MapSet) Remove(fileId int) bool {
	if m.mapBool[fileId] {
		m.mapBool[fileId] = false
		m.count--
		return true
	}
	return false
}