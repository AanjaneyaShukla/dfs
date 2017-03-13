import java.util.Scanner;

/*
 * Utilities
 */

public class DfsUtil {

	public static String coordinatorNodeIpMessage = "Enter the coordinator node server ip: ";
	public static String coordinatorNodeIpErrorMessage = "The entered coordinator node server ip is incorrect. Please try again";

	public static String coordinatorNodePortMessage = "Enter the coordinator node server port: ";
	public static String coordinatorNodePortErrorMessage = "The entered coordinator node server port number is incorrect. Please try again";

	public static String nodeIpMessage = "Enter the node server ip: ";
	public static String nodeIpErrorMessage = "The entered node server ip is incorrect. Please try again";

	public static String nodePortMessage = "Enter the node server port: ";
	public static String nodePortErrorMessage = "The entered node server port number is incorrect. Please try again";

	public static String bulkCountMessage = "Enter the bulk operation count: ";
	public static String bulkCountErrorMessage = "The entered bulk operation count is incorrect. Please try again";

	public static String bulkReadPercentMessage = "Enter the read bulk operation percent: ";
	public static String bulkReadPercentErrorMessage = "The entered read bulk operation percent is incorrect. Please try again";

	public static String fileNameMessage = "Enter the file name: ";
	public static String fileNameErrorErrorMessage = "The entered file name is incorrect. Please try again";

	public static String fileContentMessage = "Enter the file contents: ";
	public static String fileContentErrorMessage = "The entered file contents is incorrect. Please try again";

	public static String operationType = "Enter the type of operation [0=READ, 1=WRITE, 2=DFS_STRUCTURE, 3=QUIT]: ";
	public static String operationTypeError = "The entered operation type is incorrect. Please try again";

	public static String bulkOperationType = "Enter the type of operation [0=BULKREAD, 1=BULKWRITE, 2=BULKMIX, 3=DFS_STRUCTURE, 4=QUIT]: ";
	public static String bulkOperationTypeError = "The entered operation type is incorrect. Please try again";

	public static String numNodes = "Enter the value for N: ";
	public static String numNodesError = "The entered value is wrong. Please try again";

	public static String rQuorum = "Enter the value for Nr: ";
	public static String rQuorumError = "The entered value is wrong. Please try again";

	public static String wQuorum = "Enter the value for Nw: ";
	public static String wQuorumError = "The entered value is wrong. Please try again";

	public static String unableJoinCoordinator = "Unable to join the coordinator";

	public static String OK = "OK";
	public static String NOK = "NOK";

	public static String ACK = "ACK";

	public static String fileNotFound = "File not found";

	public static int N;
	public static int Nr;
	public static int Nw;
	public static int poolSize = 50;
	public static boolean debug = true;

	private static Scanner in = new Scanner(System.in);

	public static int getNumberFromUser(String message, String errorMessage) {
		int number = -1;
		while (true) {
			try {
				System.out.print(message);
				number = Integer.parseInt(in.nextLine().trim());
				if (number <= 0) {
					continue;
				}
				return number;
			} catch (NumberFormatException exp) {
				System.out.println(errorMessage);
			}
		}
	}

	public static String getStringFromUser(String message, String errorMessage) {
		String str;
		while (true) {
			System.out.print(message);
			str = in.nextLine().trim();
			if (str == null && str.isEmpty()) {
				continue;
			}
			return str;

		}
	}

	public static NodeOperations getTypeOperation() {
		int type = -1;
		while (true) {
			System.out.print(operationType);
			try {
				type = Integer.parseInt(in.nextLine());
			} catch (NumberFormatException e) {
				System.out.println("Invalid input entered. Please try again");
				continue;
			}
			switch (type) {
			case 0:
				return NodeOperations.READ;
			case 1:
				return NodeOperations.WRITE;
			case 2:
				return NodeOperations.DFSSTRUCTURE;
			case 3:
				return NodeOperations.QUIT;
			default:
				System.out.println("Wrong choice!!! Try again!!!");
				continue;
			}
		}
	}

	public static NodeOperations getBulkTypeOperation() {
		int type = -1;
		while (true) {
			System.out.print(bulkOperationType);
			try {
				type = Integer.parseInt(in.nextLine());
			} catch (NumberFormatException e) {
				System.out.println("Invalid input entered. Please try again");
				continue;
			}
			switch (type) {
			case 0:
				return NodeOperations.BULKREAD;
			case 1:
				return NodeOperations.BULKWRITE;
			case 2:
				return NodeOperations.BULKMIX;
			case 3:
				return NodeOperations.DFSSTRUCTURE;
			case 4:
				return NodeOperations.QUIT;
			default:
				System.out.println("Wrong choice!!! Try again!!!");
				continue;
			}
		}
	}

}
