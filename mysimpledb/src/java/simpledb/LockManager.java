package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class LockManager {
	
	public class LockEntry {
		private ArrayList<TransactionId> holding = new ArrayList<TransactionId>();
		private ConcurrentLinkedDeque<TransactionId> queue = new ConcurrentLinkedDeque<TransactionId>();
		private Permissions permtype = Permissions.READ_ONLY;
		private boolean inUse = false;
		private PageId pid;
		
		LockEntry(PageId pid) { this.pid = pid; }
		
		public PageId getPid() { return pid; }
		
		public boolean isInUse() { return inUse; }
		
		public boolean isReadOnly() { return permtype.equals(Permissions.READ_ONLY);  }
		
		public void setUse(TransactionId tid, boolean use, Permissions perm) {
			if (use) {
				inUse = true;
				permtype = perm;
				holding.add(tid);
				queue.remove(tid);
				
			} else {
				holding.remove(tid);
				queue.remove(tid);	// just in case!
				
				if (holding.isEmpty()) {
					permtype = Permissions.READ_ONLY;
					inUse = false;
				}
			}
		}
		
		public void setUse(TransactionId tid, boolean use) { setUse(tid, use, Permissions.READ_ONLY); }
		
		public void addToQueue(TransactionId tid, boolean isUpgrade) {
			if (!isUpgrade) { queue.addLast(tid); } 
			else { queue.addFirst(tid); }
		}
		
		public boolean isHolding(TransactionId tid) { return holding.contains(tid); }
		
		public boolean isQueued(TransactionId tid) { return queue.contains(tid); }
		
		public boolean isNext(TransactionId tid) { return queue.isEmpty() || queue.peek().equals(tid); }
		
		public boolean isUpgrade(TransactionId tid, Permissions perm) { 
			return isReadOnly() && isHolding(tid) && perm.equals(Permissions.READ_WRITE); 
		}
		
		public boolean canUpgrade(TransactionId tid, Permissions perm) {
			return isUpgrade(tid, perm) && queue.isEmpty();
		}
	}
	
	/**
	 * INSTANCE VARIALBES
	 */
	private ConcurrentHashMap<PageId,LockEntry> locktable = new ConcurrentHashMap<PageId,LockEntry>();
	private ConcurrentHashMap<TransactionId, ArrayList<PageId>> txntable = new ConcurrentHashMap<TransactionId, ArrayList<PageId>>();
	private boolean debug = true;
	
	
	public LockManager() {
		if (debug) { System.out.println("NEW LOCK MANAGER CREATED"); }
	}
	
	public void lockRequest(TransactionId tid, PageId pid, Permissions perm) {
		
		if (debug) { System.out.println("tid " + tid.toString() + ": requested lock for " + pid.toString()); }
		
		boolean waiting = true;
		
		while (waiting) {
			
			synchronized (this) {
				LockEntry lock = locktable.get(pid);
				
				// create a new lock entry if one doesn't already exist
				if (lock == null) {
					lock = new LockEntry(pid);
				}
				
				// do we already have the lock?
				if (lock.isHolding(tid) && !lock.isUpgrade(tid, perm)) {
					waiting = false;
					break;
				}
				
				// is the lock free and are we next in line?
				if (!lock.isInUse() && lock.isNext(tid)) {
					// lock is free and we are next, so grab the lock!
					lock.setUse(tid, true, perm);
					waiting = false;
					
				} else if (perm.equals(Permissions.READ_ONLY)) {
					// read-only lock requested
						
					if (lock.isInUse() && lock.isReadOnly()){
						// lock is not free, but it's got a read-only lock on it so we can use it
						lock.setUse(tid, true, perm);
						waiting = false;
						
					} else {
						// we can't get the lock at this time, so add ourselves to the queue if we're not already on it
						if (!lock.isQueued(tid)) {
							lock.addToQueue(tid, false);
						}
					}
					
				} else {
					// write lock requested
					
					if (lock.isUpgrade(tid, perm)) {
						
						if (lock.canUpgrade(tid, perm)) {
							// we are cleared to upgrade
							lock.setUse(tid, true, perm);
							waiting = false;
							
						} else {
							// we can't get the lock at this time but we're an upgrade, so jump to the head of the queue
							if (!lock.isQueued(tid)) {
								lock.addToQueue(tid, true);
							}
						}
						
					} else {
						// we can't get the lock at this time, so add ourselves to the queue if we're not already on it
						if (!lock.isQueued(tid)) {
							lock.addToQueue(tid, false);
						}
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
		
		if (debug) { System.out.println("tid " + tid.toString() + ": acquired lock for " + pid.toString()); }
		
	}
	
	public synchronized void lockRelease(TransactionId tid, PageId pid) {
		LockEntry lock = locktable.get(pid);
		
		// does this lock exist?
		if (lock == null) {
			throw new RuntimeException("the lock should exist assuming we aren't calling lockRelease before lockRequest");
		}

		// do we even have the lock?
		if (!lock.isHolding(tid)) {
			return;
		}
		
		// update lock entry and lock table
		lock.setUse(tid, false);
		locktable.put(pid, lock);
		
		if (debug) { System.out.println("tid" + tid.toString() + ": released lock for " + pid.toString()); }
	}
	
	public synchronized boolean hasLock(TransactionId tid, PageId pid) {
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
	
}