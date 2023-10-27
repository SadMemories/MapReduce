package mr

import "sync"
import "errors"

type ListNode struct {
	data interface{}
	next *ListNode
	prev *ListNode
}

func (node *ListNode) addBefore(data interface{}) {
	newNode := ListNode {}
	newNode.data = data

	prev := node.prev
	node.prev = &newNode
	newNode.next = node
	newNode.prev = prev
	prev.next = &newNode
}

func (node *ListNode) addAfter(data interface{}) {
	newNode := ListNode {}
	newNode.data = data

	next := node.next 
	node.next = &newNode
	newNode.prev = node
	newNode.next = next
	next.prev = &newNode
}

func (node *ListNode) removeBefore() {
	prev := node.prev.prev

	node.prev = prev
	prev.next = node
} 

func (node *ListNode) removeAfter() {
	next := node.next.next
	
	node.next = next
	next.prev = node
}

type LinkedList struct {
	head ListNode
	count int
}

func newLinkedList() *LinkedList {
	link := LinkedList {}
	link.count = 0
	
	link.head.next = &link.head
	link.head.prev = &link.head

	return &link
}

func (link *LinkedList) pushFront(data interface{}) {
	link.head.addAfter(data)
	link.count++
}

func (link *LinkedList) pushBack(data interface{}) {
	link.head.addBefore(data)
	link.count++
}

func (link *LinkedList) popFront() (interface {}, error) {
	if link.count == 0 {
		return nil, errors.New("popping Front empty list")
	}

	data := link.head.next.data
	link.head.removeAfter()
	link.count--

	return data, nil
}

func (link *LinkedList) popBack() (interface {}, error) {
	if link.count == 0 {
		return nil, errors.New("popping Back empty list")
	}

	data := link.head.prev.data
	link.head.removeBefore()
	link.count--

	return data, nil
}

func (link *LinkedList) peekFront() (interface {}, error) {
	if link.count == 0 {
		return nil, errors.New("peek Front empty list")
	}

	data := link.head.next.data
	return data, nil
}

func (link *LinkedList) peekBack() (interface {}, error) {
	if link.count == 0 {
		return nil, errors.New("peek Back empty list")
	}

	data := link.head.prev.data
	return data, nil
}

func (link *LinkedList) size() int {
	return link.count
}

type BlockQueue struct {
	link *LinkedList
	cond *sync.Cond
}

func NewBlockQueue() *BlockQueue {
	queue := BlockQueue {}
	queue.link = newLinkedList()
	queue.cond = sync.NewCond(new(sync.Mutex))

	return &queue
}

func (queue *BlockQueue) PutFront(data interface{}) {
	queue.cond.L.Lock()
	queue.link.pushFront(data)
	queue.cond.L.Unlock()
	queue.cond.Broadcast();
}

func (queue *BlockQueue) PutBack(data interface{}) {
	queue.cond.L.Lock()
	queue.link.pushBack(data)
	queue.cond.L.Unlock()
	queue.cond.Broadcast()
}

func (queue *BlockQueue) GetFront() (interface {}, error) {
	queue.cond.L.Lock()
	if queue.link.size() == 0 {
		queue.cond.Wait()
	}
	data, err:= queue.link.popFront()
	queue.cond.L.Unlock()
	return data, err
}

func (queue *BlockQueue) GetBack() (interface {}, error) {
	queue.cond.L.Lock()
	if queue.link.size() == 0 {
		queue.cond.Wait()
	}
	data, err:= queue.link.popBack()
	queue.cond.L.Unlock()
	return data, err
}

func (queue *BlockQueue) PopFront() (interface {}, error) {
	queue.cond.L.Lock()
	data, err:= queue.link.popFront()
	queue.cond.L.Unlock()
	return data, err
}

func (queue *BlockQueue) PopBack() (interface {}, error) {
	queue.cond.L.Lock()
	data, err:= queue.link.popBack()
	queue.cond.L.Unlock()
	return data, err
}

func (queue *BlockQueue) Size() int {
	return queue.link.size()
}

func (queue *BlockQueue) PeekFront() (interface {}, error) {
	queue.cond.L.Lock()
	if queue.link.size() == 0 {
		queue.cond.Wait()
	}
	data, err:= queue.link.peekFront()
	queue.cond.L.Unlock()
	return data, err
}

func (queue *BlockQueue) peekBack() (interface {}, error) {
	queue.cond.L.Lock()
	if queue.link.size() == 0 {
		queue.cond.Wait()
	}
	data, err:= queue.link.peekBack()
	queue.cond.L.Unlock()
	return data, err
}
