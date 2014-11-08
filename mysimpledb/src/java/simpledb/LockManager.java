package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class LockManager {
	
	public class LockEntry {
		private ArrayList<TransactionId> holding = new ArrayList<TransactionId>();
		private ConcurrentLinkedQueue<TransactionId> queue = new ConcurrentLinkedQueue<TransactionId>();
		private char locktype = 0;
		private boolean inUse = false;
		private PageId pid;
		
		LockEntry(PageId pid) { this.pid = pid; }
		
		public PageId getPid() { return pid; }
		
		public boolean isInUse() { return inUse; }
		
		public void setUse(TransactionId tid, boolean use) {
			if (use) {
				inUse = true;
				holding.add(tid);
				queue.remove(tid);
			} else {
				inUse = false;
				holding.remove(tid);
				queue.remove(tid);
			}
		}
		
		public void addToQueue(TransactionId tid) { queue.add(tid); }
		
		public boolean isHolding(TransactionId tid) { return holding.contains(tid); }
		
		public boolean isQueued(TransactionId tid) { return queue.contains(tid); }
		
		public boolean isNext(TransactionId tid) { return queue.isEmpty() || queue.peek().equals(tid);  }
	}
	
	/**
	 * INSTANCE VARIALBES
	 */
	private ConcurrentHashMap<PageId,LockEntry> locktable = new ConcurrentHashMap<PageId,LockEntry>();
	private ConcurrentHashMap<TransactionId, ArrayList<PageId>> txntable = new ConcurrentHashMap<TransactionId, ArrayList<PageId>>();
	
	public void lockRequest(TransactionId tid, PageId pid, char type) {
		
		System.out.println("tid " + tid.toString() + ": requested lock for " + pid.toString());
		
		boolean waiting = true;
		
		while (waiting) {
			
			synchronized (this) {
				LockEntry lock = locktable.get(pid);
				
				// create a new lock entry if one doesn't already exist
				if (lock == null) {
					lock = new LockEntry(pid);
				}
				
				// do we already have the lock?
				if (lock.isHolding(tid)) {
					waiting = false;
				}
				
				// is the lock free and are we next in line?
				if (!lock.isInUse() && lock.isNext(tid)) {
					// lock is free and we are next, so grab the lock!
					lock.setUse(tid, true);
					waiting = false;
					
				} else {
					// lock is not free, so add ourselves to the queue if we're not already on it
					if (!lock.isQueued(tid)) {
						lock.addToQueue(tid);
					}
				}
				
				locktable.put(pid, lock);
			}
			
			// spin wait if necessary
			if (waiting) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {}
			}
		}
		
		System.out.println("tid " + tid.toString() + ": acquired lock for " + pid.toString());
		
	}
	
	public void lockRelease(TransactionId tid, PageId pid) {
		LockEntry lock = locktable.get(pid);

		// do we even have the lock?
		if (!lock.isHolding(tid)) {
			return;
		}
		
		// update lock entry and lock table
		synchronized (this) {
			if (lock != null) {
				lock.setUse(tid, false);
				locktable.put(pid, lock);
			}
		}
		
		System.out.println("tid" + tid.toString() + ": released lock for " + pid.toString());
	}
	
	public boolean hasLock(TransactionId tid, PageId pid) {
		LockEntry lock = locktable.get(pid);
		
		if (lock != null) {
			return lock.isHolding(tid);
		}
		
		return false;
	}
	
	public void transactionCommit(TransactionId tid) {
		throw new UnsupportedOperationException();
	}
	
	public void transactionAbort(TransactionId tid) {
		throw new UnsupportedOperationException();
	}
	
	public void lockUpgrade(int oid) {
		throw new UnsupportedOperationException();
	}
	
}