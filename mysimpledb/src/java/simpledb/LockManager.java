package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.HashSet;

public class LockManager {
	
	public class TransactionEntry {
		private TransactionId tid;
		private HashSet<PageId> holding = new HashSet<PageId>();
		private HashSet<PageId> waiting = new HashSet<PageId>();
		private long endTime;
		
		static final long maxTime = 50;
		
		TransactionEntry(TransactionId tid) { 
			this.tid = tid; 
			endTime = System.currentTimeMillis() + maxTime; 
		}
		
		public TransactionId getTid() { return tid; }
		
		public void addHolding(PageId pid) { 
			holding.add(pid);
			waiting.remove(pid);
		}
		
		public void addWaiting(PageId pid) { waiting.add(pid); }
		
		public void removeHolding(PageId pid) { holding.remove(pid); }
		
		public HashSet<PageId> getWait() { return waiting; }
		
		public HashSet<PageId> getHold() { return holding; }
		
		public boolean isExpired() { return (endTime < System.currentTimeMillis()); }

	}
	
	public class LockEntry {
		private HashSet<TransactionId> holding = new HashSet<TransactionId>();
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
			return isUpgrade(tid, perm) && (holding.size() == 1);
		}
		
		public void removeQueued(TransactionId tid) { queue.remove(tid); }
	}
	
	/**
	 * INSTANCE VARIALBES
	 */
	private ConcurrentHashMap<PageId,LockEntry> locktable = new ConcurrentHashMap<PageId,LockEntry>();
	private ConcurrentHashMap<TransactionId, TransactionEntry> transtable = new ConcurrentHashMap<TransactionId, TransactionEntry>();
	private boolean debug = false;
	
	public LockManager() {
		if (debug) { System.out.println("NEW LOCK MANAGER CREATED"); }
	}
	
	public void lockRequest(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
				
		// double check permissions in case I screwed up
		if (!(perm.equals(Permissions.READ_ONLY) || perm.equals(Permissions.READ_WRITE))) {
			throw new RuntimeException("make sure permissions are either READ_ONLY or READ_WRITE");
		}
		
		boolean waiting = true;
		
		while (waiting) {
			
			synchronized (this) {
				LockEntry lock = locktable.get(pid);
				TransactionEntry trans = transtable.get(tid);
				
				// create a new lock entry if one doesn't already exist
				if (lock == null) {
					lock = new LockEntry(pid);
				}
				
				// create a new transaction entry if one doesn't already exist
				if (trans == null) {
					trans = new TransactionEntry(tid);
				}
				
				// do we already have the lock?
				if (lock.isHolding(tid) && !lock.isUpgrade(tid, perm)) {
					waiting = false;
					break;
				}
				
				if (debug) { System.out.println("tid " + tid.toString() + ": requested " + perm.toString() + " lock for " + pid.toString()); }
				
				// is the lock free and are we next in line?
				if (!lock.isInUse() && lock.isNext(tid)) {
					// lock is free and we are next, so grab the lock!
					lock.setUse(tid, true, perm);
					trans.addHolding(pid);
					waiting = false;
					
					if (debug) { System.out.println("tid " + tid.toString() + ": acquired " + perm.toString() + " lock for " + pid.toString()); }

					
				} else if (perm.equals(Permissions.READ_ONLY)) {
					// read-only lock requested
						
					if (lock.isInUse() && lock.isReadOnly()) {
						// lock is not free, but it's got a read-only lock on it so we can use it
						lock.setUse(tid, true, perm);
						trans.addHolding(pid);
						waiting = false;
						
						if (debug) { System.out.println("tid " + tid.toString() + ": acquired READ_ONLY lock for " + pid.toString()); }

					} else {
						// check for deadlock
						if (trans.isExpired()) {
							throw new TransactionAbortedException();
						}
						
						// we can't get the lock at this time, so add ourselves to the queue if we're not already on it
						if (!lock.isQueued(tid)) {
							lock.addToQueue(tid, false);
							trans.addWaiting(pid);
						}
					}
					
				} else {
					// write lock requested
					
					if (lock.isUpgrade(tid, perm)) {
						
						if (lock.canUpgrade(tid, perm)) {
							// we are cleared to upgrade
							lock.setUse(tid, true, perm);
							trans.addHolding(pid);	// just in case!
							waiting = false;
							
							if (debug) { System.out.println("tid " + tid.toString() + ": acquired READ_WRITE lock for " + pid.toString()); }
							
						} else {
							// check for deadlock
							if (trans.isExpired()) {
								throw new TransactionAbortedException();
							}
							
							// we can't get the lock at this time but we're an upgrade, so jump to the head of the queue
							if (!lock.isQueued(tid)) {
								lock.addToQueue(tid, true);
								trans.addWaiting(pid);
							}
						}
						
					} else {
						// check for deadlock
						if (trans.isExpired()) {
							throw new TransactionAbortedException();
						}
						
						// we can't get the lock at this time, so add ourselves to the queue if we're not already on it
						if (!lock.isQueued(tid)) {
							lock.addToQueue(tid, false);
							trans.addWaiting(pid);
						}
					}
				}
				
				locktable.put(pid, lock);
				transtable.put(tid, trans);
			}
			
			// spin wait if necessary
			if (waiting) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {}
			}
		}
				
	}
	
	public synchronized void lockRelease(TransactionId tid, PageId pid) {
		LockEntry lock = locktable.get(pid);
		TransactionEntry trans = transtable.get(tid);
		
		// does this lock exist?
		if (lock == null) {
			throw new RuntimeException("the lock should exist assuming we aren't calling lockRelease before lockRequest");
		}

		// do we even have the lock?
		if (!lock.isHolding(tid)) {
			return;
		}
		
		// update lock entry, lock table, transaction entry, and transaction table
		lock.setUse(tid, false);
		trans.removeHolding(pid);
		locktable.put(pid, lock);
		transtable.put(tid, trans);
		
		if (debug) { System.out.println("tid " + tid.toString() + ": released lock for " + pid.toString()); }
	}
	
	public synchronized boolean hasLock(TransactionId tid, PageId pid) {
		LockEntry lock = locktable.get(pid);
		
		if (lock != null) {
			return lock.isHolding(tid);
		}
		
		return false;
	}
	
	public synchronized PageId[] getHolding(TransactionId tid) {
		if (transtable.containsKey(tid)) {
			return transtable.get(tid).getHold().toArray(new PageId[0]);
		}
		return null;
	}
	
	public synchronized void transactionCommit(TransactionId tid) {
		if (!transtable.containsKey(tid)) {
			return;		// don't execute this method twice!
		}
		
		TransactionEntry trans = transtable.get(tid);
		
		// clear out any locks that we were waiting for
		Iterator<PageId> waitit= trans.getWait().iterator();
		
		while (waitit.hasNext()) {
			PageId pid = waitit.next();
			
			LockEntry lock = locktable.get(pid);
			lock.removeQueued(tid);
			locktable.put(pid, lock);
		}
		
		// release the locks we are holding
		Iterator<PageId> holdit = trans.getHold().iterator();
		
		while (holdit.hasNext()) {
			PageId pid = holdit.next();
			
			LockEntry lock = locktable.get(pid);
			lock.setUse(tid, false);
			locktable.put(pid, lock);
			
			if (debug) { System.out.println("tid " + tid.toString() + ": released lock for " + pid.toString()); }
			
		}
		
		// delete the transaction object
		transtable.remove(tid);
		
		if (debug) { System.out.println("tid " + tid.toString() + ": closed"); }
	}
	
	public synchronized void transactionAbort(TransactionId tid) {
		transactionCommit(tid);
	}
	
}