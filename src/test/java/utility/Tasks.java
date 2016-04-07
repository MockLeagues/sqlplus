package utility;

public class Tasks {

	@FunctionalInterface
	public static interface Task {
		
		public void run() throws Exception;
		
	}
	
	public static double timeSeconds(Task t) throws Exception {
		long start = System.currentTimeMillis();
		t.run();
		long end = System.currentTimeMillis();
		return (end - start) / 1000.0;
	}
	
}
