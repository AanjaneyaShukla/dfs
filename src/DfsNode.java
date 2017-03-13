import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

/*
 * Dfs Node Server start-up
 */

public class DfsNode {

	private static boolean isCoordinator = false;
	private static int coordinatorServerPort = -1;
	private static int nodeServerPort = -1;
	private static String coordinatorIp;
	private static String clientIp;
	public static NodeDetail coordinatorNode = null;
	private static long syncDelay = 60000;

	private static void setNodeServerPort() {
		nodeServerPort = DfsUtil.getNumberFromUser(DfsUtil.nodePortMessage, DfsUtil.nodePortErrorMessage);
	}

	private static NodeDetail setCoordinatorNodeDetail() {
		coordinatorIp = DfsUtil.getStringFromUser(DfsUtil.coordinatorNodeIpMessage,
				DfsUtil.coordinatorNodeIpErrorMessage);
		coordinatorServerPort = DfsUtil.getNumberFromUser(DfsUtil.coordinatorNodePortMessage,
				DfsUtil.coordinatorNodePortErrorMessage);
		NodeDetail coordinatorNode = new NodeDetail(coordinatorIp, coordinatorServerPort);
		return coordinatorNode;
	}

	public static void main(String[] args) throws Exception {
		clientIp = InetAddress.getLocalHost().getHostAddress();
		if (args.length != 0) {
			switch (NodeType.convertStrToEnum(args[0])) {
			case Coordinator:
				System.out.println("Starting the co-ordinator server");
				isCoordinator = true;
				coordinatorServerPort = DfsUtil.getNumberFromUser(DfsUtil.coordinatorNodePortMessage,
						DfsUtil.coordinatorNodePortErrorMessage);
				DfsUtil.N = DfsUtil.getNumberFromUser(DfsUtil.numNodes, DfsUtil.numNodesError);
				do {
					DfsUtil.Nr = DfsUtil.getNumberFromUser(DfsUtil.rQuorum, DfsUtil.rQuorumError);
					DfsUtil.Nw = DfsUtil.getNumberFromUser(DfsUtil.wQuorum, DfsUtil.wQuorumError);
					if (!((DfsUtil.Nr + DfsUtil.Nw > DfsUtil.N) && (DfsUtil.Nw > (DfsUtil.N / 2.0))
							&& (DfsUtil.Nr <= DfsUtil.N) && (DfsUtil.Nw <= DfsUtil.N))) {
						System.out.println(DfsUtil.rQuorumError);
					}
				} while (!((DfsUtil.Nr + DfsUtil.Nw > DfsUtil.N) && (DfsUtil.Nw > (DfsUtil.N / 2.0))
						&& (DfsUtil.Nr <= DfsUtil.N) && (DfsUtil.Nw <= DfsUtil.N)));
				new Thread() {
					@Override
					public void run() {
						try {
							startCoordinator();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}.start();
				new Timer().scheduleAtFixedRate(new TimerTask() {
					@Override
					public void run() {
						if (DfsUtil.debug)
							System.out.println("Sync Started");
						for (Iterator<Map.Entry<PendingOperation, FileContent>> itr = DfsNodeClientImpl
								.getPendingSyncDetail().entrySet().iterator(); itr.hasNext();) {
							Entry<PendingOperation, FileContent> entry = itr.next();
							DfsNodeClientImpl.forwardRequestNode(
									new Operation(NodeOperations.WRITE, entry.getKey().getFileName(),
											entry.getValue().getContents()),
									entry.getKey().getNode().getIp(), entry.getKey().getNode().getPort(),
									entry.getValue().getVersion(), true);
							itr.remove();
						}

					}
				}, syncDelay, syncDelay);
				break;
			default:
				break;
			}
		}
		if (DfsUtil.debug) {
			System.out.println("Adding the node to the co-ordinator server");
		}
		if (!isCoordinator)
			coordinatorNode = setCoordinatorNodeDetail();
		else {
			coordinatorIp = clientIp;
			coordinatorNode = new NodeDetail(coordinatorIp, coordinatorServerPort);
		}
		startNodeClient(coordinatorNode);
	}

	public static void startCoordinator() {
		TServerTransport serverTransport = null;
		DfsNodeClientImpl handler = null;
		try {
			serverTransport = new TServerSocket(coordinatorServerPort);
			handler = new DfsNodeClientImpl();
		} catch (Exception e) {
			e.printStackTrace();
		}
		TTransportFactory factory = new TFramedTransport.Factory();
		DfsNodeClient.Processor<DfsNodeClientImpl> processor = new DfsNodeClient.Processor<>(handler);
		TThreadPoolServer.Args arguments = new TThreadPoolServer.Args(serverTransport);
		arguments.processor(processor);
		arguments.transportFactory(factory);
		TServer server = new TThreadPoolServer(arguments);
		server.serve();
	}

	public static void startNodeServer() throws Exception {
		TServerTransport serverTransport = null;
		DfsNodeServerImpl handler = null;
		try {
			serverTransport = new TServerSocket(nodeServerPort);
			handler = new DfsNodeServerImpl();
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		TTransportFactory factory = new TFramedTransport.Factory();
		DfsClient.Processor<DfsNodeServerImpl> processor = new DfsClient.Processor<>(handler);
		TThreadPoolServer.Args arguments = new TThreadPoolServer.Args(serverTransport);
		arguments.processor(processor);
		arguments.transportFactory(factory);
		TServer server = new TThreadPoolServer(arguments);
		server.serve();
	}

	public static void startNodeClient(NodeDetail coordinatorNode) throws Exception {
		TTransport transport = null;
		try {
			transport = new TSocket(coordinatorNode.getIp(), coordinatorNode.getPort());
			TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
			DfsNodeClient.Client client = new DfsNodeClient.Client(protocol);
			transport.open();
			setNodeServerPort();
			NodeDetail node = new NodeDetail(clientIp, nodeServerPort);
			if (client.join(node.getIp(), node.getPort()).equals(DfsUtil.OK)) {
				new Thread() {
					@Override
					public void run() {
						try {
							startNodeServer();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}.start();
			} else {
				System.out.println(DfsUtil.unableJoinCoordinator);
			}
		} catch (TException e) {
			e.printStackTrace();
		} finally {
			// System.out.println("Closing the connection");
			if (transport != null)
				transport.close();
		}
	}
}
