package com.grpctest.mapreduce.driver;



public class CicularList {
    class Node {

        int data;
        Node next;

        public Node(int data) {
            this.data = data;
        }
    }

    // declaring head pointer as null
    public Node head = null;
    public Node tempo = null;

    // function adds new nodes at the end of list
    public void addNode(int data) {
        Node new1 = new Node(data);

        // If linked list is empty
        if (head == null) {
            head = new1;
        } else {
            tempo.next = new1;
        }

        tempo = new1;

        // last node points to first node
        tempo.next = head;
    }

    public Integer  next() {
        // temp will traverse the circular
        Node temp = head;

        if (head == null) {
            return null;
        } else {
            Integer elem = head.data;
            head = head.next;
            return elem;
        }
    }
}