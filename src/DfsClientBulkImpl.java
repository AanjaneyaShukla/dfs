import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;

public class DfsClientBulkImpl {

	private static String[] file = { "test1.txt", "test2.txt", "test3.txt", "test4.txt", "test5.txt", "test6.txt",
			"test7.txt", "test8.txt" };
	private static AtomicInteger[] content = { new AtomicInteger(), new AtomicInteger(), new AtomicInteger(),
			new AtomicInteger(), new AtomicInteger(), new AtomicInteger(), new AtomicInteger(), new AtomicInteger() };
	private static Random rand = new Random();
	private static int bulkCount = 1000;
	private static int bulkReadPercent = 50;

	private static ExecutorService clientService = new ThreadPoolExecutor(DfsUtil.poolSize, DfsUtil.poolSize, 0L,
			TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

	private static Future<?> readExecuteOp(final NodeDetail node) {
		return clientService.submit(new Runnable() {
			@Override
			public void run() {
				try {
					DfsClientImpl.execute(false, node, true, file[rand.nextInt(file.length)], null,
							NodeOperations.READ);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	private static Future<?> writeExecuteOp(final NodeDetail node) {
		return clientService.submit(new Runnable() {
			@Override
			public void run() {
				try {
					int fileIndex = rand.nextInt(file.length);
					DfsClientImpl.execute(false, node, true, file[fileIndex],
							Integer.toString(content[fileIndex].incrementAndGet()), NodeOperations.WRITE);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	public static void execute(NodeDetail node) {
		boolean loop = true;
		do {
			long startTime = -1;
			List<Future<?>> listOfTasks = new ArrayList<>();
			switch (DfsUtil.getBulkTypeOperation()) {
			case BULKREAD:
				startTime = System.currentTimeMillis();
				for (int i = 0; i < bulkCount; i++) {
					listOfTasks.add(readExecuteOp(node));
				}
				break;
			case BULKWRITE:
				startTime = System.currentTimeMillis();
				for (int i = 0; i < bulkCount; i++) {
					listOfTasks.add(writeExecuteOp(node));
				}
				break;
			case BULKMIX:
				startTime = System.currentTimeMillis();
				bulkReadPercent = DfsUtil.getNumberFromUser(DfsUtil.bulkReadPercentMessage,
						DfsUtil.bulkReadPercentErrorMessage);
				int bulkReadCount = 0;
				int bulkWriteCount = 0;
				while (bulkWriteCount < (int) (((100 - bulkReadPercent) / 100.0f) * bulkCount)
						&& bulkReadCount < (int) ((bulkReadPercent / 100.0f) * bulkCount)) {
					if (rand.nextBoolean()) {
						listOfTasks.add(readExecuteOp(node));
						bulkReadCount++;
					} else {
						listOfTasks.add(writeExecuteOp(node));
						bulkWriteCount++;
					}
				}
				while (bulkReadCount < (int) ((bulkReadPercent / 100.0f) * bulkCount)) {
					listOfTasks.add(readExecuteOp(node));
					bulkReadCount++;
				}
				while (bulkWriteCount < (int) (((100 - bulkReadPercent) / 100.0f) * bulkCount)) {
					listOfTasks.add(writeExecuteOp(node));
					bulkWriteCount++;
				}
				break;
			case DFSSTRUCTURE:
				startTime = System.currentTimeMillis();
				try {
					DfsClientImpl.execute(false, node, true, "", "", NodeOperations.DFSSTRUCTURE);
				} catch (TException e1) {
					e1.printStackTrace();
				}
				break;
			case QUIT:
				loop = false;
				break;
			default:
				break;
			}
			for (Future<?> task : listOfTasks) {
				try {
					task.get();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
			System.out.println("The time taken to execute the process is : " + (System.currentTimeMillis() - startTime) + "ms");
		} while (loop);
	}

	public static void main(String[] args) throws TException {
		System.out.println("Starting Client...");
		DfsUtil.debug = false;
		bulkCount = DfsUtil.getNumberFromUser(DfsUtil.bulkCountMessage, DfsUtil.bulkCountErrorMessage);
		NodeDetail node = DfsClientImpl.setNodeDetail();
		execute(node);
        System.exit(0);
	}
}
