import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/*
 * External Client Implementation
 */

public class DfsClientImpl {

	public static NodeDetail setNodeDetail() {
		String nodeServerIp = DfsUtil.getStringFromUser(DfsUtil.nodeIpMessage, DfsUtil.nodeIpErrorMessage);
		int nodeServerPort = DfsUtil.getNumberFromUser(DfsUtil.nodePortMessage, DfsUtil.nodePortErrorMessage);
		return new NodeDetail(nodeServerIp, nodeServerPort);
	}

	public static void execute(boolean loop, NodeDetail node, boolean isBulkOp, String fileName, String fileContent,
			NodeOperations op) throws TException {
		TTransport transport = null;
		try {
			transport = new TSocket(node.getIp(), node.getPort());
			TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
			final DfsClient.Client client = new DfsClient.Client(protocol);
			transport.open();
			do {
				if (!isBulkOp)
					op = DfsUtil.getTypeOperation();
				switch (op) {
				case READ:
					if (!isBulkOp)
						fileName = DfsUtil.getStringFromUser(DfsUtil.fileNameMessage,
								DfsUtil.fileNameErrorErrorMessage);
					String readResponse = client.read(fileName);
					if (DfsUtil.debug) {
						System.out.println(fileName + " : " + readResponse);
					}
					break;
				case WRITE:
					if (!isBulkOp) {
						fileName = DfsUtil.getStringFromUser(DfsUtil.fileNameMessage,
								DfsUtil.fileNameErrorErrorMessage);
						fileContent = DfsUtil.getStringFromUser(DfsUtil.fileContentMessage,
								DfsUtil.fileContentErrorMessage);
					}
					String writeResponse = client.write(fileName, fileContent);
					if (DfsUtil.debug) {
						System.out.println(writeResponse);
					}
					break;
				case DFSSTRUCTURE:
					System.out.println(client.getDfsStructure());
					break;
				case QUIT:
					loop = false;
					break;
				default:
					break;
				}
			} while (loop);
		} catch (TException e) {
			e.printStackTrace();
			throw e;
		} finally {
			// System.out.println("Closing the connection");
			if (transport != null)
				transport.close();
		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Starting Client...");
		NodeDetail node = setNodeDetail();
		execute(true, node, false, null, null, null);
	}
}
