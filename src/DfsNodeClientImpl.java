import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/*
 * Coordinator API implementation
 */

public class DfsNodeClientImpl implements DfsNodeClient.Iface {

	private static Set<NodeDetail> nodeDetails = new HashSet<>();
	private static ExecutorService service = new ThreadPoolExecutor(DfsUtil.poolSize, DfsUtil.poolSize, 0L,
			TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
	private static Map<PendingOperation, FileContent> pendingSyncDetail = new ConcurrentHashMap<>();

	private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

	public static String forwardRequestNode(Operation op, String ip, int port, int version, boolean isSync) {
		TTransport transport = null;
		try {
			transport = new TSocket(ip, port);
			TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
			DfsClient.Client client = new DfsClient.Client(protocol);
			transport.open();
			switch (op.op) {
			case WRITE:
				// System.out.println(op.fileName + " " + op.contents + " " + ip
				// + " " + port);
				return client.writeCoordinator(op.fileName, op.contents, version, isSync);
			case READ:
				// System.out.println(op.fileName + " " + ip + " " + port);
				FileContent fileContent = client.readCoordinator(op.fileName);
				if (fileContent != null) {
					return fileContent.getVersion() + "|" + fileContent.getContents();
				} else {
					if (DfsUtil.debug) {
						System.out.println(DfsUtil.fileNotFound);
					}
					return DfsUtil.fileNotFound;
				}
			case VERSION:
				return Integer.toString(client.getVersion(op.fileName));
			case DFSSTRUCTURE:
				return client.getDfsStructureCoordinator();
			default:
				break;
			}
		} catch (TException e) {
			e.printStackTrace();
		} finally {
			// System.out.println("Closing the connection");
			if (transport != null)
				transport.close();
		}
		return null;
	}

	private static String startProcess(final Operation op) {
		switch (op.getOp()) {
		case READ:
			lock.readLock().lock();
			Set<NodeDetail> readNodes = new HashSet<>();
			List<Future<String>> listOfReadTasks = new ArrayList<>();
			Random readRand = new Random();
			while (readNodes.size() < DfsUtil.Nr) {
				readNodes.add((NodeDetail) nodeDetails.toArray()[readRand.nextInt(nodeDetails.size())]);
			}

			Iterator<NodeDetail> readItr = readNodes.iterator();
			while (readItr.hasNext()) {
				final NodeDetail temp = readItr.next();
				Future<String> future = service.submit(new Callable<String>() {
					@Override
					public String call() throws Exception {
						return forwardRequestNode(op, temp.getIp(), temp.getPort(), -1, false);
					}
				});
				listOfReadTasks.add(future);
			}
			int tempCurrentVersion = -2;
			String currentFileContent = null;
			for (Future<String> future : listOfReadTasks) {
				String readResponse = null;
				try {
					readResponse = future.get();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
				if (readResponse != null) {
					int temp = Integer.parseInt(readResponse.substring(0, readResponse.indexOf('|')));
					if (temp > tempCurrentVersion) {
						tempCurrentVersion = temp;
						currentFileContent = readResponse.substring(readResponse.indexOf('|') + 1,
								readResponse.length());
					}
				}
			}
			lock.readLock().unlock();
			return currentFileContent;
		case WRITE:
			lock.writeLock().lock();
			Set<NodeDetail> writeNodes = new HashSet<>();
			List<Future<String>> listOfVersionTasks = new ArrayList<>();
			List<Future<String>> listOfWriteTasks = new ArrayList<>();
			Random writeRand = new Random();
			while (writeNodes.size() < DfsUtil.Nw) {
				writeNodes.add((NodeDetail) nodeDetails.toArray()[writeRand.nextInt(nodeDetails.size())]);
			}

			Iterator<NodeDetail> versionItr = writeNodes.iterator();
			int version = -1;

			while (versionItr.hasNext()) {
				final NodeDetail temp = versionItr.next();
				Future<String> future = service.submit(new Callable<String>() {
					@Override
					public String call() throws Exception {
						return forwardRequestNode(new Operation(NodeOperations.VERSION, op.fileName, op.contents),
								temp.getIp(), temp.getPort(), -1, false);
					}
				});
				listOfVersionTasks.add(future);
			}
			for (Future<String> future : listOfVersionTasks) {
				try {
					int temp = new Integer(future.get());
					if (temp > version) {
						version = temp;
					}
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}

			Iterator<NodeDetail> writeItr = writeNodes.iterator();
			Set<NodeDetail> syncWriteNodes = new HashSet<>();
			syncWriteNodes.addAll(nodeDetails);
			syncWriteNodes.removeAll(writeNodes);

			final int currentVersion = version + 1;
			Iterator<NodeDetail> itr = syncWriteNodes.iterator();
			while (itr.hasNext()) {
				pendingSyncDetail.put(new PendingOperation(itr.next(), op.fileName),
						new FileContent(op.contents, currentVersion));
			}
			if (DfsUtil.debug) {
				//System.out.println(pendingSyncDetail);
			}

			while (writeItr.hasNext()) {
				final NodeDetail nodeDetailItr = writeItr.next();
				pendingSyncDetail.remove(new PendingOperation(nodeDetailItr, op.fileName));
				Future<String> future = service.submit(new Callable<String>() {
					@Override
					public String call() throws Exception {
						return forwardRequestNode(op, nodeDetailItr.getIp(), nodeDetailItr.getPort(), currentVersion,
								false);
					}
				});
				listOfWriteTasks.add(future);
			}
			for (Future<String> future : listOfWriteTasks) {
				try {
					future.get();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
			lock.writeLock().unlock();
			return DfsUtil.ACK;
		case DFSSTRUCTURE:
			lock.readLock().lock();
			String dfsStructureResponse = null;
			List<Future<String>> listOfDfsStructureTasks = new ArrayList<>();
			Map<Future<String>, String> dfsStructure = new HashMap<>();
			Iterator<NodeDetail> dfsStructureItr = nodeDetails.iterator();
			while (dfsStructureItr.hasNext()) {
				final NodeDetail temp = dfsStructureItr.next();
				Future<String> future = service.submit(new Callable<String>() {
					@Override
					public String call() throws Exception {
						return forwardRequestNode(op, temp.getIp(), temp.getPort(), -1, false);
					}
				});
				listOfDfsStructureTasks.add(future);
				dfsStructure.put(future, temp.getIp() + ":" + temp.getPort());
			}
			for (Future<String> future : listOfDfsStructureTasks) {
				try {
					String temp = future.get();
					if (dfsStructureResponse == null)
						dfsStructureResponse = dfsStructure.get(future) + " " + temp + " ";
					else
						dfsStructureResponse += dfsStructure.get(future) + " " + temp + " ";
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
			lock.readLock().unlock();
			return dfsStructureResponse;
		default:
			break;
		}
		return DfsUtil.fileNotFound;
	}

	@Override
	public String join(String ip, int port) throws InvalidAddress, InvalidNode, TException {
		if (nodeDetails.add(new NodeDetail(ip, port))) {
			if (DfsUtil.debug) {
				System.out.println(nodeDetails);
			}
			return DfsUtil.OK;
		} else
			return DfsUtil.NOK;
	}

	@Override
	public String write(String fileName, String contents) throws TException {
		return startProcess(new Operation(NodeOperations.WRITE, fileName, contents));
	}

	@Override
	public String read(String fileName) throws TException {
		return startProcess(new Operation(NodeOperations.READ, fileName, ""));
	}

	@Override
	public String getDfsStructure() throws TException {
		return startProcess(new Operation(NodeOperations.DFSSTRUCTURE, "", ""));
	}

	public static Map<PendingOperation, FileContent> getPendingSyncDetail() {
		return pendingSyncDetail;
	}
}
