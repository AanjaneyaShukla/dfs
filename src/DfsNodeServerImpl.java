import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/*
 * Node API implementation
 */

public class DfsNodeServerImpl implements DfsClient.Iface {

	private static Map<String, FileContent> file = new ConcurrentHashMap<>();

	public static String forwardRequestCoordinator(Operation op) {
		TTransport transport = null;
		try {
			transport = new TSocket(DfsNode.coordinatorNode.getIp(), DfsNode.coordinatorNode.getPort());
			TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
			DfsNodeClient.Client client = new DfsNodeClient.Client(protocol);
			transport.open();
			switch (op.op) {
			case WRITE:
				// System.out.println(op.fileName + " " + op.contents);
				return client.write(op.fileName, op.contents);
			case READ:
				// System.out.println(op.fileName);
				return client.read(op.fileName);
			case DFSSTRUCTURE:
				return client.getDfsStructure();
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
		return DfsUtil.fileNotFound;
	}

	@Override
	public String write(String fileName, String contents) throws TException {
		return forwardRequestCoordinator(new Operation(NodeOperations.WRITE, fileName, contents));
	}

	@Override
	public String read(String fileName) throws TException {
		return forwardRequestCoordinator(new Operation(NodeOperations.READ, fileName, null));
	}

	@Override
	public String getDfsStructure() throws TException {
		return forwardRequestCoordinator(new Operation(NodeOperations.DFSSTRUCTURE, null, null));
	}

	@Override
	public int getVersion(String fileName) throws TException {
		if (file.containsKey(fileName))
			return file.get(fileName).getVersion();
		return -1;
	}

	@Override
	public FileContent readCoordinator(String fileName) throws TException {
		if (file.containsKey(fileName)) {
			if (DfsUtil.debug) {
				//System.out.println(file.get(fileName));
			}
			return file.get(fileName);
		} else {
			if (DfsUtil.debug) {
				//System.out.println(DfsUtil.fileNotFound);
			}
			return new FileContent(DfsUtil.fileNotFound, -1);
		}
	}

	@Override
	public String writeCoordinator(String fileName, String contents, int version, boolean isSync) throws TException {
		if (DfsUtil.debug) {
			//System.out.println("Writing to the file " + fileName + " with contents " + contents);
		}
		if (isSync) {
			if (file.containsKey(fileName)) {
				FileContent content = file.get(fileName);
				if (content != null && content.getVersion() > version) {
					file.put(fileName, new FileContent(contents, version));
				}
			} else {
				file.put(fileName, new FileContent(contents, version));
			}
		}
		file.put(fileName, new FileContent(contents, version));
		if (DfsUtil.debug) {
			//System.out.println(file);
		}
		return DfsUtil.ACK;
	}

	@Override
	public String getDfsStructureCoordinator() throws TException {
		return file.toString();
	}

}
